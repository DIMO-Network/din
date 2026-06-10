package attest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestVerifier() *Verifier {
	return NewVerifierWithBackend(nil, zerolog.Nop())
}

func TestParse(t *testing.T) {
	now := time.Now().UTC()
	attestationTimestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)

	tests := []struct {
		name             string
		inputData        []byte
		jwtAddress       common.Address
		expectedError    bool
		expectedType     string
		expectedProducer string
		expectedSubject  string
		expectedSource   string
	}{
		{
			name:             "successful attestation",
			inputData:        []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError:    false,
			expectedType:     cloudevent.TypeAttestation,
			expectedProducer: "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
			expectedSubject:  "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
			expectedSource:   common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").Hex(),
		},
		{
			name:          "attestation with dimo.raw.* type",
			inputData:     []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.raw.insurance","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: false,
			expectedType:  "dimo.raw.insurance",
		},
		{
			name:          "attestation with dimo.document.* type",
			inputData:     []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.document.vehicle.registration","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: false,
			expectedType:  "dimo.document.vehicle.registration",
		},
		{
			name:             "attestation payload source differs from JWT source (delegation)",
			inputData:        []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:       common.HexToAddress("0xABCDEF1234567890ABCDEF1234567890ABCDEF12"), // JWT holder differs from payload source
			expectedError:    false,
			expectedType:     cloudevent.TypeAttestation,
			expectedProducer: "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
			expectedSubject:  "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
			expectedSource:   common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").Hex(),
		},
		{
			name:             "attestation empty payload source falls back to JWT",
			inputData:        []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:       common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError:    false,
			expectedType:     cloudevent.TypeAttestation,
			expectedProducer: "0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b",
			expectedSubject:  "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005",
			expectedSource:   common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").Hex(),
		},
		{
			name:          "attestation invalid payload source",
			inputData:     []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"not-an-address","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: true,
		},
		{
			name: "attestation with oversized Extras exceeds size cap",
			inputData: []byte(fmt.Sprintf(
				`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","junk":"%s","data":{"x":1}}`,
				attestationTimestamp.Format(time.RFC3339),
				strings.Repeat("z", convert.MaxHeaderBytes+1),
			)),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: true,
		},
		{
			name:          "attestation with invalid type",
			inputData:     []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.signals","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","insured":true,"provider":"State Farm","coverageStartDate":1744751357,"expirationDate":1807822654,"policyNumber":"SF-12345678"}}`, attestationTimestamp.Format(time.RFC3339))),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: true,
		},
		{
			name:          "attestation with future timestamp",
			inputData:     []byte(fmt.Sprintf(`{"id":"unique-attestation-id-1","source":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","producer":"0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xa2f41b51853db03749da01976aaef503252c3e240e4edb3c5651856c7b4842fa54be0cb843ee380561f5583ed7b38c99f8db6f3d3aa345856449e85be6e29af91b","data":{"x":1}}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))),
			jwtAddress:    common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b"),
			expectedError: true,
		},
	}

	verifier := newTestVerifier()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attestation, err := verifier.Parse(context.Background(), tt.jwtAddress, tt.inputData)
			if tt.expectedError {
				require.Error(t, err, "expected error but got none")
				require.ErrorIs(t, err, ErrValidation, "expected a 400-class validation error")
				return
			}
			require.NoError(t, err, "unexpected error: %v", err)
			require.NotNil(t, attestation)

			assert.Equal(t, tt.expectedType, attestation.Type)
			if tt.expectedProducer != "" {
				assert.Equal(t, tt.expectedProducer, attestation.Producer)
			}
			if tt.expectedSubject != "" {
				assert.Equal(t, tt.expectedSubject, attestation.Subject)
			}
			if tt.expectedSource != "" {
				assert.Equal(t, tt.expectedSource, attestation.Source)
			}
			assert.Empty(t, attestation.VoidsID, "non-tombstone attestations must not carry a VoidsID")
		})
	}
}

func TestParseAndValidateAttestationContentType(t *testing.T) {
	source := common.HexToAddress("0x07B584f6a7125491C991ca2a45ab9e641B1CeE1b").String()
	timestamp := time.Now().UTC().Format(time.RFC3339)
	baseFields := func(extra string) []byte {
		return []byte(fmt.Sprintf(`{"id":"id1","source":"%s","producer":"%s","specversion":"1.0","subject":"did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005","time":"%s","type":"dimo.attestation","signature":"0xdeadbeef"%s}`, source, source, timestamp, extra))
	}

	tests := []struct {
		name        string
		input       []byte
		expectError bool
	}{
		{
			name:  "data event with no datacontenttype defaults to json",
			input: baseFields(`,"data":{"k":"v"}`),
		},
		{
			name:        "data event with image/png is rejected",
			input:       baseFields(`,"data":{"k":"v"},"datacontenttype":"image/png"`),
			expectError: true,
		},
		{
			name:  "data_base64 event with image/png is accepted",
			input: baseFields(`,"data_base64":"aGVsbG8=","datacontenttype":"image/png"`),
		},
		{
			name:        "data_base64 event without datacontenttype is rejected",
			input:       baseFields(`,"data_base64":"aGVsbG8="`),
			expectError: true,
		},
		{
			name:        "data_base64 event with non-whitelisted type is rejected",
			input:       baseFields(`,"data_base64":"aGVsbG8=","datacontenttype":"text/plain"`),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseAndValidateAttestation(tt.input, source)
			if tt.expectError {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsValidAttestationType(t *testing.T) {
	t.Parallel()

	valid := []string{
		cloudevent.TypeAttestation,
		cloudevent.TypeAttestationTombstone,
		"dimo.raw.insurance",
		"dimo.document.vehicle.registration",
	}
	for _, ty := range valid {
		assert.True(t, isValidAttestationType(ty), "expected %q to be valid", ty)
	}

	invalid := []string{
		"",
		"dimo.raw.",
		"dimo.document.",
		"dimo.signals",
		"dimo.status",
		"random",
	}
	for _, ty := range invalid {
		assert.False(t, isValidAttestationType(ty), "expected %q to be invalid", ty)
	}
}

func TestParseTombstoneData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		data            string
		expectVoidsID   string
		expectErr       bool
		expectErrSubstr string
	}{
		{
			name:          "valid tombstone",
			data:          `{"voidsId":"target-id-1","reason":"uploaded by mistake"}`,
			expectVoidsID: "target-id-1",
		},
		{
			name:          "valid tombstone without reason",
			data:          `{"voidsId":"target-id-1"}`,
			expectVoidsID: "target-id-1",
		},
		{
			name:            "empty data",
			data:            ``,
			expectErr:       true,
			expectErrSubstr: "data payload is required",
		},
		{
			name:            "data is not an object",
			data:            `"just a string"`,
			expectErr:       true,
			expectErrSubstr: "not a tombstone object",
		},
		{
			name:            "missing voidsId",
			data:            `{"reason":"oops"}`,
			expectErr:       true,
			expectErrSubstr: "voidsId is required",
		},
		{
			name:            "empty voidsId",
			data:            `{"voidsId":"","reason":"oops"}`,
			expectErr:       true,
			expectErrSubstr: "voidsId is required",
		},
		{
			name:            "voidsId with disallowed character",
			data:            `{"voidsId":"bad id$"}`,
			expectErr:       true,
			expectErrSubstr: "invalid voidsId",
		},
		{
			name:            "reason exceeds cap",
			data:            fmt.Sprintf(`{"voidsId":"target-id-1","reason":"%s"}`, strings.Repeat("a", MaxTombstoneReasonBytes+1)),
			expectErr:       true,
			expectErrSubstr: "reason length",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			voidsID, err := parseTombstoneData(json.RawMessage(tt.data))
			if tt.expectErr {
				require.Error(t, err)
				if tt.expectErrSubstr != "" {
					assert.Contains(t, err.Error(), tt.expectErrSubstr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectVoidsID, voidsID)
		})
	}
}

// signTombstoneEvent builds a signed dimo.tombstone CloudEvent JSON using
// the provided ECDSA key. The returned bytes match the wire format that DIN
// would receive over the attestation endpoint, with a real EOA signature
// over the data field.
func signTombstoneEvent(t *testing.T, key string, source common.Address, subject, id, voidsID, reason string, ts time.Time) []byte {
	t.Helper()
	privKey, err := crypto.HexToECDSA(key)
	require.NoError(t, err)

	dataObj := map[string]string{"voidsId": voidsID}
	if reason != "" {
		dataObj["reason"] = reason
	}
	dataBytes, err := json.Marshal(dataObj)
	require.NoError(t, err)

	hash := accounts.TextHash(dataBytes)
	sig, err := crypto.Sign(hash, privKey)
	require.NoError(t, err)
	// crypto.Sign returns v as 0/1; convert to Ethereum's 27/28.
	sig[64] += 27

	envelope := map[string]any{
		"id":          id,
		"source":      source.Hex(),
		"producer":    source.Hex(),
		"specversion": "1.0",
		"subject":     subject,
		"time":        ts.Format(time.RFC3339),
		"type":        cloudevent.TypeAttestationTombstone,
		"signature":   "0x" + common.Bytes2Hex(sig),
		"data":        json.RawMessage(dataBytes),
	}
	out, err := json.Marshal(envelope)
	require.NoError(t, err)
	return out
}

func TestParse_Tombstone(t *testing.T) {
	t.Parallel()

	// Deterministic test key so the test is reproducible.
	const privHex = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
	privKey, err := crypto.HexToECDSA(privHex)
	require.NoError(t, err)
	source := crypto.PubkeyToAddress(privKey.PublicKey)

	subject := "did:erc721:80002:0x45fbCD3ef7361d156e8b16F5538AE36DEdf61Da8:1005"
	now := time.Now().UTC().Truncate(time.Minute)

	t.Run("valid tombstone is accepted and carries VoidsID", func(t *testing.T) {
		input := signTombstoneEvent(t, privHex, source, subject, "tombstone-id-1", "target-attestation-id-1", "uploaded in error", now)

		verifier := newTestVerifier()
		attestation, err := verifier.Parse(context.Background(), source, input)
		require.NoError(t, err, "unexpected error: %v", err)
		require.NotNil(t, attestation)

		assert.Equal(t, "target-attestation-id-1", attestation.VoidsID)
		assert.Equal(t, cloudevent.TypeAttestationTombstone, attestation.Type)
	})

	t.Run("tombstone with empty voidsId in data is rejected", func(t *testing.T) {
		// Manually craft a tombstone where voidsId is empty but the signature
		// still recovers (we sign over the actual empty-voidsId data).
		dataObj := map[string]string{"voidsId": "", "reason": "x"}
		dataBytes, err := json.Marshal(dataObj)
		require.NoError(t, err)
		hash := accounts.TextHash(dataBytes)
		sig, err := crypto.Sign(hash, privKey)
		require.NoError(t, err)
		sig[64] += 27
		envelope := map[string]any{
			"id":          "tombstone-id-2",
			"source":      source.Hex(),
			"producer":    source.Hex(),
			"specversion": "1.0",
			"subject":     subject,
			"time":        now.Format(time.RFC3339),
			"type":        cloudevent.TypeAttestationTombstone,
			"signature":   "0x" + common.Bytes2Hex(sig),
			"data":        json.RawMessage(dataBytes),
		}
		input, err := json.Marshal(envelope)
		require.NoError(t, err)

		verifier := newTestVerifier()
		_, err = verifier.Parse(context.Background(), source, input)
		require.Error(t, err, "expected error for empty voidsId")
		require.ErrorIs(t, err, ErrValidation)
	})

	// Bad-signature behavior is identical for all attestation flavors and is
	// covered by the existing TestParse cases for `dimo.attestation`;
	// no need to duplicate it here.
}
