package admit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/DIMO-Network/did-directory/pkg/dpop"
)

// Refusal is the org host declining: no grant lets the source write about
// the vehicle. Code is the host's refusal code (not_covered, exclusive_hold).
type Refusal struct {
	Code    string
	Message string
}

func (r *Refusal) Error() string {
	return fmt.Sprintf("not authorized: %s (%s)", r.Message, r.Code)
}

// OrgHost asks a did-directory org host whether a source may write about a
// vehicle, with POST /authorize and anyGrant, so the host finds the grant.
// din calls as its own DID, which the host must list among its issuers.
type OrgHost struct {
	// BaseURL is the host, without a trailing slash.
	BaseURL string
	// Identity supplies din's identity token (a signin.Client's Token).
	Identity func(ctx context.Context) (string, error)
	// Key is the DPoP key that token is bound to.
	Key *dpop.Key
	// HTTP is the client to send with; nil means a 10s-timeout client.
	HTTP *http.Client
}

type authorizeRequest struct {
	AnyGrant  bool     `json:"anyGrant"`
	Caller    string   `json:"caller"`
	Vehicle   string   `json:"vehicle"`
	Abilities []string `json:"abilities"`
}

// Authorize returns the URI of the grant that lets source exercise abilities
// on vehicle now, a *Refusal when none does, or an error wrapping
// ErrUnavailable when the host gave no answer.
func (h *OrgHost) Authorize(ctx context.Context, source, vehicle string, abilities []string) (string, error) {
	token, err := h.Identity(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: din's identity token: %v", ErrUnavailable, err)
	}
	body, err := json.Marshal(authorizeRequest{AnyGrant: true, Caller: source, Vehicle: vehicle, Abilities: abilities})
	if err != nil {
		return "", err
	}
	url := strings.TrimRight(h.BaseURL, "/") + "/authorize"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	proof, err := h.Key.Proof(http.MethodPost, url, token, time.Now())
	if err != nil {
		return "", fmt.Errorf("%w: DPoP proof: %v", ErrUnavailable, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", dpop.TokenType+" "+token)
	req.Header.Set(dpop.HeaderName, proof)
	client := h.HTTP
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: org host: %v", ErrUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("%w: org host: %v", ErrUnavailable, err)
	}

	var answer struct {
		Chain []string `json:"chain"`
		Error string   `json:"error"`
		Code  string   `json:"code"`
	}
	_ = json.Unmarshal(data, &answer)
	switch {
	case resp.StatusCode == http.StatusOK:
		if len(answer.Chain) == 0 {
			return "", fmt.Errorf("%w: org host answered with no chain", ErrUnavailable)
		}
		return answer.Chain[0], nil
	case resp.StatusCode == http.StatusForbidden && answer.Code != "":
		return "", &Refusal{Code: answer.Code, Message: answer.Error}
	default:
		// Includes a 403 with no code, which is the host refusing din itself
		// (not configured as an issuer): an outage, not the source's fault.
		return "", fmt.Errorf("%w: org host: %s: %s", ErrUnavailable, resp.Status, strings.TrimSpace(string(data)))
	}
}

var _ Authorizer = (*OrgHost)(nil)

// errorsAsRefusal reports whether err is a *Refusal.
func errorsAsRefusal(err error) bool {
	var r *Refusal
	return errors.As(err, &r)
}
