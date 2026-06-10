package attest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	checksumAddr = "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d"
	lowerAddr    = "0x06012c8cf97bead5deae237070f9587f8e7a266d"
)

func TestParseAndValidateAttestation_ChecksumsSubjectAndSource(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)

	tests := []struct {
		name            string
		event           cloudevent.CloudEventHeader
		callerSource    string
		expectedSubject string
		expectedSource  string
	}{
		{
			name: "lowercased erc721 subject is rewritten to checksum",
			event: cloudevent.CloudEventHeader{
				ID:          "id-1",
				SpecVersion: "1.0",
				Source:      lowerAddr,
				Producer:    lowerAddr,
				Subject:     "did:erc721:1:" + lowerAddr + ":1005",
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:erc721:1:" + checksumAddr + ":1005",
			expectedSource:  checksumAddr,
		},
		{
			name: "lowercased ethr subject is rewritten to checksum",
			event: cloudevent.CloudEventHeader{
				ID:          "id-2",
				SpecVersion: "1.0",
				Source:      lowerAddr,
				Producer:    lowerAddr,
				Subject:     "did:ethr:1:" + lowerAddr,
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:ethr:1:" + checksumAddr,
			expectedSource:  checksumAddr,
		},
		{
			name: "lowercased caller source falls back when payload source is empty",
			event: cloudevent.CloudEventHeader{
				ID:          "id-3",
				SpecVersion: "1.0",
				Producer:    lowerAddr,
				Subject:     "did:erc721:1:" + lowerAddr + ":1",
				Type:        cloudevent.TypeAttestation,
				Time:        now,
			},
			callerSource:    lowerAddr,
			expectedSubject: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:  checksumAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := cloudevent.RawEvent{CloudEventHeader: tt.event, Data: json.RawMessage(`{}`)}
			msgBytes, err := json.Marshal(raw)
			require.NoError(t, err)

			got, err := parseAndValidateAttestation(msgBytes, tt.callerSource)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedSubject, got.Subject)
			assert.Equal(t, tt.expectedSource, got.Source)
		})
	}
}
