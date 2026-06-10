package convert

import (
	"testing"

	"github.com/DIMO-Network/cloudevent"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	checksumAddr = "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d"
	lowerAddr    = "0x06012c8cf97bead5deae237070f9587f8e7a266d"
)

func TestCanonicalizeConnectionHeader_ChecksumsAddresses(t *testing.T) {
	logger := zerolog.Nop()

	tests := []struct {
		name             string
		hdr              cloudevent.CloudEventHeader
		expectedSubject  string
		expectedProducer string
		expectedSource   string
	}{
		{
			name: "lowercased erc721 DID is rewritten to checksum",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:erc721:1:" + lowerAddr + ":2",
				Producer: "did:erc721:1:" + lowerAddr + ":1",
				Source:   lowerAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
		{
			name: "already-checksummed erc721 DID is unchanged",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:erc721:1:" + checksumAddr + ":2",
				Producer: "did:erc721:1:" + checksumAddr + ":1",
				Source:   checksumAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
		{
			name: "legacy nft DID with lowercased address is rewritten to checksummed erc721",
			hdr: cloudevent.CloudEventHeader{
				Subject:  "did:nft:1:" + lowerAddr + "_2",
				Producer: "did:nft:1:" + lowerAddr + "_1",
				Source:   lowerAddr,
			},
			expectedSubject:  "did:erc721:1:" + checksumAddr + ":2",
			expectedProducer: "did:erc721:1:" + checksumAddr + ":1",
			expectedSource:   checksumAddr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := tt.hdr
			require.True(t, canonicalizeConnectionHeader(&hdr, logger))
			assert.Equal(t, tt.expectedSubject, hdr.Subject)
			assert.Equal(t, tt.expectedProducer, hdr.Producer)
			assert.Equal(t, tt.expectedSource, hdr.Source)
		})
	}
}
