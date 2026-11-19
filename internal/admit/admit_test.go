package admit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/did-directory/pkg/didkey"
	"github.com/DIMO-Network/did-directory/pkg/dpop"
	"github.com/DIMO-Network/did-directory/pkg/provenance"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	relay = "did:dimo:flespi"
	car   = "did:dimo:car1"
	grant = "at://did:dimo:owner/dimo.delegation/source"
)

// keys maps a kid to its public key, standing in for the directory.
type keys map[string]*ecdsa.PublicKey

func (k keys) Key(_ context.Context, kid string) (*didkey.PublicKey, error) {
	pub, ok := k[kid]
	if !ok {
		return nil, provenance.ErrUnknownKey
	}
	return didkey.FromECDSA(pub)
}

// grants answers like the org host: a grant for the pairs it holds, a
// refusal otherwise, or an outage when down.
type grants struct {
	held  map[string]string // vehicle -> grant
	down  bool
	calls int
}

func (g *grants) Authorize(_ context.Context, source, vehicle string, abilities []string) (string, error) {
	g.calls++
	if g.down {
		return "", fmt.Errorf("%w: connection refused", ErrUnavailable)
	}
	if source == relay && len(abilities) == 1 && abilities[0] == "telemetry:write" {
		if uri, ok := g.held[vehicle]; ok {
			return uri, nil
		}
	}
	return "", &Refusal{Code: "not_covered", Message: "no grant covers " + vehicle}
}

type fixture struct {
	t       *testing.T
	admit   *Admitter
	grants  *grants
	relay   provenance.Signer
	machine provenance.Signer
	mid     string
	now     time.Time
}

func newFixture(t *testing.T) *fixture {
	relayKey := newKey(t)
	machineKey := newKey(t)
	mid, err := didkey.Encode(&machineKey.PublicKey)
	require.NoError(t, err)
	mkid := mid + "#" + strings.TrimPrefix(mid, "did:key:")
	g := &grants{held: map[string]string{car: grant}}
	f := &fixture{
		t: t, grants: g, mid: mid,
		relay:   provenance.Signer{Key: relayKey, KID: relay + "#signing"},
		machine: provenance.Signer{Key: machineKey, KID: mkid},
		now:     time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC),
	}
	f.admit = &Admitter{
		Keys:       keys{relay + "#signing": &relayKey.PublicKey, mkid: &machineKey.PublicKey},
		Authorizer: g,
		Now:        func() time.Time { return f.now },
	}
	return f
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return k
}

// data has odd spacing on purpose: the signature covers these exact bytes.
const data = `{"signals": [ {"name":"speed","timestamp":"2026-10-27T08:59:00Z","value":42} ]}`

type event struct {
	hdr   provenance.Header
	extra map[string]any
}

func (f *fixture) event(subject string) event {
	return event{hdr: provenance.Header{
		ID: "evt-" + subject, Source: relay, Producer: f.mid, Subject: subject,
		Type: "dimo.status", Time: time.Date(2026, 10, 27, 8, 59, 0, 0, time.UTC),
		DataVersion: "default/v1.0", DataContentType: "application/json",
	}, extra: map[string]any{}}
}

// signed adds both signatures over e as it stands.
func (f *fixture) signed(e event) event {
	for _, s := range []struct {
		signer provenance.Signer
		attr   string
	}{{f.relay, provenance.SourceSignatureAttr}, {f.machine, provenance.ProducerSignatureAttr}} {
		jws, err := s.signer.Sign(e.hdr, []byte(data))
		require.NoError(f.t, err)
		e.extra[s.attr] = jws
	}
	return e
}

func (e event) json() string {
	m := map[string]any{
		"specversion": "1.0", "id": e.hdr.ID, "source": e.hdr.Source, "producer": e.hdr.Producer,
		"subject": e.hdr.Subject, "type": e.hdr.Type, "dataversion": e.hdr.DataVersion,
		"datacontenttype": e.hdr.DataContentType,
	}
	if !e.hdr.Time.IsZero() {
		m["time"] = e.hdr.Time.Format(time.RFC3339Nano)
	}
	for k, v := range e.extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	// data goes in verbatim, as a sender's encoder would write it.
	return strings.TrimSuffix(string(b), "}") + `,"data":` + data + "}"
}

func TestAdmit_StoresTheEventAsSentPlusArrivalAndGrant(t *testing.T) {
	f := newFixture(t)
	e := f.signed(f.event(car))
	e.extra[ArrivedAtAttr] = "1999-01-01T00:00:00Z" // a source cannot pick these
	e.extra[GrantAttr] = "at://did:dimo:x/dimo.delegation/forged"

	events, err := f.admit.Admit(context.Background(), relay, []byte(e.json()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	ev := events[0]
	assert.Equal(t, data, string(ev.Data), "data is stored byte for byte")
	assert.Equal(t, relay, ev.Source)
	assert.Equal(t, f.mid, ev.Producer)
	assert.Equal(t, "dimo.status", ev.Type, "type is the sender's, not inferred")
	assert.Equal(t, "2026-10-27T09:00:00Z", ev.Extras[ArrivedAtAttr])
	assert.Equal(t, grant, ev.Extras[GrantAttr])
	assert.Equal(t, e.extra[provenance.SourceSignatureAttr], ev.Extras[provenance.SourceSignatureAttr], "signatures are kept")
	assert.Equal(t, e.extra[provenance.ProducerSignatureAttr], ev.Extras[provenance.ProducerSignatureAttr])

	// What is stored still verifies.
	h := provenance.Header{ID: ev.ID, Source: ev.Source, Producer: ev.Producer, Subject: ev.Subject,
		Type: ev.Type, Time: ev.Time, DataVersion: ev.DataVersion, DataContentType: ev.DataContentType}
	_, err = provenance.Verify(context.Background(), f.admit.Keys, h, ev.Data, provenance.Signatures{
		Source: ev.Extras[provenance.SourceSignatureAttr].(string), Producer: ev.Extras[provenance.ProducerSignatureAttr].(string),
	})
	assert.NoError(t, err)
}

func TestAdmit_SourceMustBeTheCertificates(t *testing.T) {
	f := newFixture(t)
	_, err := f.admit.Admit(context.Background(), "did:dimo:someoneelse", []byte(f.signed(f.event(car)).json()))
	assert.ErrorIs(t, err, ErrForbidden)
}

func TestAdmit_Signatures(t *testing.T) {
	f := newFixture(t)
	e := f.signed(f.event(car))
	delete(e.extra, provenance.ProducerSignatureAttr)
	_, err := f.admit.Admit(context.Background(), relay, []byte(e.json()))
	assert.ErrorIs(t, err, ErrForbidden, "a DID producer must sign")

	e = f.signed(f.event(car))
	e.hdr.Time = e.hdr.Time.Add(time.Second) // signed for another time
	_, err = f.admit.Admit(context.Background(), relay, []byte(e.json()))
	assert.ErrorIs(t, err, ErrForbidden)
	assert.Zero(t, f.grants.calls, "signatures are checked before the host is asked")
}

func TestAdmit_Grants(t *testing.T) {
	f := newFixture(t)
	other := f.signed(f.event("did:dimo:car2"))
	_, err := f.admit.Admit(context.Background(), relay, []byte(other.json()))
	assert.ErrorIs(t, err, ErrForbidden)
	var refusal *Refusal
	assert.ErrorAs(t, err, &refusal)

	// Answers are reused for a while, refusals for less.
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(car)).json()))
	require.NoError(t, err)
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(car)).json()))
	require.NoError(t, err)
	assert.Equal(t, 2, f.grants.calls)

	f.now = f.now.Add(2 * time.Minute)
	delete(f.grants.held, car) // revoked
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(car)).json()))
	assert.ErrorIs(t, err, ErrForbidden, "a revoked grant stops writes once the answer expires")

	// An outage is not remembered as a refusal.
	f.now = f.now.Add(time.Minute)
	f.grants.down = true
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(car)).json()))
	assert.ErrorIs(t, err, ErrUnavailable)
	f.grants.down = false
	f.grants.held[car] = grant
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(car)).json()))
	assert.NoError(t, err)
}

func TestAdmit_FillsInNothing(t *testing.T) {
	f := newFixture(t)
	e := f.signed(f.event(car))
	e.hdr.Time = time.Time{} // sent without one
	_, err := f.admit.Admit(context.Background(), relay, []byte(e.json()))
	assert.ErrorIs(t, err, convert.ErrValidation, "din may not fill in a signed time")

	e = f.event(car)
	e.hdr.DataContentType = ""
	_, err = f.admit.Admit(context.Background(), relay, []byte(f.signed(e).json()))
	assert.ErrorIs(t, err, convert.ErrValidation)
}

func TestAdmit_SubjectIsADID(t *testing.T) {
	f := newFixture(t)
	for _, s := range []string{"did:dimo:../../x", "did:dimo:a/b", "car1", "did:dimo:"} {
		_, err := f.admit.Admit(context.Background(), relay, []byte(f.signed(f.event(s)).json()))
		assert.ErrorIs(t, err, convert.ErrValidation, s)
	}
}

func TestAdmit_BatchIsAllOrNothing(t *testing.T) {
	f := newFixture(t)
	good := f.signed(f.event(car)).json()
	body := "[" + good + "," + good + "]"
	events, err := f.admit.Admit(context.Background(), relay, []byte(body))
	require.NoError(t, err)
	assert.Len(t, events, 2)

	body = "[" + good + "," + f.signed(f.event("did:dimo:car2")).json() + "]"
	_, err = f.admit.Admit(context.Background(), relay, []byte(body))
	assert.ErrorIs(t, err, ErrForbidden)

	_, err = f.admit.Admit(context.Background(), relay, []byte("[]"))
	assert.ErrorIs(t, err, convert.ErrValidation)
}

func TestOrgHost(t *testing.T) {
	key, err := dpop.GenerateKey()
	require.NoError(t, err)
	var got authorizeRequest
	status, answer := http.StatusOK, `{"chain":["`+grant+`"],"live":["telemetry:write"]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/authorize", r.URL.Path)
		assert.Equal(t, "DPoP din-token", r.Header.Get("Authorization"))
		_, err := dpop.NewVerifier().Verify(r.Header.Get("DPoP"), r.Method, "http://"+r.Host+r.URL.Path, "din-token")
		assert.NoError(t, err)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(answer))
	}))
	defer srv.Close()
	host := &OrgHost{BaseURL: srv.URL, Key: key, Identity: func(context.Context) (string, error) { return "din-token", nil }}

	uri, err := host.Authorize(context.Background(), relay, car, []string{"telemetry:write"})
	require.NoError(t, err)
	assert.Equal(t, grant, uri)
	assert.Equal(t, authorizeRequest{AnyGrant: true, Caller: relay, Vehicle: car, Abilities: []string{"telemetry:write"}}, got)

	status, answer = http.StatusForbidden, `{"error":"no grant","code":"not_covered"}`
	_, err = host.Authorize(context.Background(), relay, car, []string{"telemetry:write"})
	var refusal *Refusal
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "not_covered", refusal.Code)

	// The host refusing din itself is din's problem, not the source's.
	status, answer = http.StatusForbidden, `{"error":"only a configured issuer may call /authorize"}`
	_, err = host.Authorize(context.Background(), relay, car, []string{"telemetry:write"})
	assert.ErrorIs(t, err, ErrUnavailable)
	assert.False(t, errors.As(err, &refusal))
}
