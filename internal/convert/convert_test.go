package convert

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCloudEventModule struct {
	hdrs []cloudevent.CloudEventHeader
	data []byte
	err  error
}

func (m *mockCloudEventModule) CloudEventConvert(ctx context.Context, data []byte) ([]cloudevent.CloudEventHeader, []byte, error) {
	return m.hdrs, m.data, m.err
}

func TestConvert(t *testing.T) {
	timestamp := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		setupMock      func() *mockCloudEventModule
		name           string
		sourceID       string
		inputData      []byte
		eventLen       int
		expectedError  bool
		expectedFirst  *cloudevent.CloudEventHeader
		expectedSecond *cloudevent.CloudEventHeader
	}{
		{
			name:      "successful event conversion",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
				}
				event2 := event
				event2.Type = cloudevent.TypeFingerprint
				data := json.RawMessage(`{"key": "value"}`)

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event, event2},
					data: data,
					err:  nil,
				}
			},
			eventLen:      2,
			expectedError: false,
			expectedFirst: &cloudevent.CloudEventHeader{
				Type:     cloudevent.TypeStatus,
				Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
			},
			expectedSecond: &cloudevent.CloudEventHeader{
				Type:     cloudevent.TypeFingerprint,
				Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
			},
		},
		{
			name:      "successful event conversion with legacy NFT DID",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_1",
					Subject:  "did:nft:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d_2",
					Time:     timestamp,
				}
				event2 := event
				event2.Type = cloudevent.TypeFingerprint
				data := json.RawMessage(`{"key": "value"}`)

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event, event2},
					data: data,
					err:  nil,
				}
			},
			eventLen:      2,
			expectedError: false,
			expectedFirst: &cloudevent.CloudEventHeader{
				Type:     cloudevent.TypeStatus,
				Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
			},
		},
		{
			name:      "future timestamp is only a warning",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     time.Now().Add(time.Hour),
				}
				data := json.RawMessage(`{"key": "value"}`)

				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: data,
					err:  nil,
				}
			},
			eventLen:      1,
			expectedError: false,
		},
		{
			name:      "conversion error",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{
					hdrs: nil,
					data: nil,
					err:  errors.New("conversion failed"),
				}
			},
			expectedError: true,
		},
		{
			name:      "invalid cloud event format",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  "test-source",
			setupMock: func() *mockCloudEventModule {
				return &mockCloudEventModule{
					hdrs: nil,
					data: json.RawMessage(`invalid json`),
					err:  nil,
				}
			},
			expectedError: true,
		},
		{
			name:      "unsupported connection event type",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeAttestation,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			expectedError: true,
		},
		{
			name:      "connection header with oversized Tags exceeds size cap",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
					Tags:     []string{strings.Repeat("x", MaxHeaderBytes+1)},
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			expectedError: true,
		},
		{
			name:      "connection header with many small Tags totaling above cap",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				tags := make([]string, 1000)
				for i := range tags {
					tags[i] = strings.Repeat("a", 16)
				}
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
					Tags:     tags,
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			expectedError: true,
		},
		{
			name:      "connection header with Tags just under size cap succeeds",
			inputData: []byte(`{"test": "data"}`),
			sourceID:  common.HexToAddress("0x").String(),
			setupMock: func() *mockCloudEventModule {
				// Aim for ~7 KiB of tag content; well under the 8 KiB cap.
				event := cloudevent.CloudEventHeader{
					ID:       "33",
					Type:     cloudevent.TypeStatus,
					Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
					Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
					Time:     timestamp,
					Tags:     []string{strings.Repeat("y", 7000)},
				}
				return &mockCloudEventModule{
					hdrs: []cloudevent.CloudEventHeader{event},
					data: json.RawMessage(`{"key": "value"}`),
					err:  nil,
				}
			},
			eventLen:      1,
			expectedError: false,
			expectedFirst: &cloudevent.CloudEventHeader{
				Type:     cloudevent.TypeStatus,
				Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
				Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
			},
		},
	}

	converter := NewConverter(zerolog.Nop(), Config{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock
			mockModule := tt.setupMock()
			modules.CloudEventRegistry.Override(tt.sourceID, mockModule)

			events, err := converter.Convert(context.Background(), tt.sourceID, tt.inputData)
			if tt.expectedError {
				require.Error(t, err, "expected error but got none")
				require.ErrorIs(t, err, ErrValidation, "expected a 400-class validation error")
				return
			}
			require.NoError(t, err, "unexpected error: %v", err)
			require.Len(t, events, tt.eventLen, "unexpected number of events")

			// Multi-header fan-out shares the payload.
			if len(mockModule.data) > 0 {
				for _, ev := range events {
					assert.Equal(t, json.RawMessage(mockModule.data), ev.Data)
				}
			}

			if tt.expectedFirst != nil {
				assert.Equal(t, tt.expectedFirst.Type, events[0].Type)
				assert.Equal(t, tt.expectedFirst.Producer, events[0].Producer)
				assert.Equal(t, tt.expectedFirst.Subject, events[0].Subject)
				assert.Equal(t, tt.sourceID, events[0].Source)
			}
			if tt.expectedSecond != nil {
				require.GreaterOrEqual(t, len(events), 2)
				assert.Equal(t, tt.expectedSecond.Type, events[1].Type)
				assert.Equal(t, tt.expectedSecond.Producer, events[1].Producer)
				assert.Equal(t, tt.expectedSecond.Subject, events[1].Subject)
			}
			// Defaults are always filled in.
			for _, ev := range events {
				assert.NotEmpty(t, ev.ID)
				assert.Equal(t, "1.0", ev.SpecVersion)
				assert.False(t, ev.Time.IsZero())
				assert.Equal(t, "application/json", ev.DataContentType)
			}
		})
	}
}

func TestConvertSharedDefaultID(t *testing.T) {
	// All events fanned out from one payload share the same default ID.
	sourceID := common.HexToAddress("0x1111111111111111111111111111111111111111").String()
	hdr := cloudevent.CloudEventHeader{
		Type:     cloudevent.TypeStatus,
		Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
		Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
		Time:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	hdr2 := hdr
	hdr2.Type = cloudevent.TypeFingerprint
	modules.CloudEventRegistry.Override(sourceID, &mockCloudEventModule{
		hdrs: []cloudevent.CloudEventHeader{hdr, hdr2},
		data: []byte(`{"key": "value"}`),
	})

	converter := NewConverter(zerolog.Nop(), Config{})
	events, err := converter.Convert(context.Background(), sourceID, []byte(`{"test": "data"}`))
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, events[0].ID, events[1].ID)
}

func TestConvertEmptyModuleDataFallsBackToInput(t *testing.T) {
	sourceID := common.HexToAddress("0x2222222222222222222222222222222222222222").String()
	hdr := cloudevent.CloudEventHeader{
		Type:     cloudevent.TypeStatus,
		Producer: "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:1",
		Subject:  "did:erc721:1:0x06012c8cf97BEaD5deAe237070F9587f8E7A266d:2",
		Time:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	modules.CloudEventRegistry.Override(sourceID, &mockCloudEventModule{
		hdrs: []cloudevent.CloudEventHeader{hdr},
	})

	input := []byte(`{"test": "data"}`)
	converter := NewConverter(zerolog.Nop(), Config{})
	events, err := converter.Convert(context.Background(), sourceID, input)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, json.RawMessage(input), events[0].Data)
}

func TestValidateAndSetContentType(t *testing.T) {
	tests := []struct {
		name                string
		inputContentType    string
		isBase64            bool
		expectedContentType string
		expectError         bool
	}{
		{
			name:                "data event empty defaults to application/json",
			inputContentType:    "",
			isBase64:            false,
			expectedContentType: "application/json",
		},
		{
			name:                "data event with application/json passes",
			inputContentType:    "application/json",
			isBase64:            false,
			expectedContentType: "application/json",
		},
		{
			name:             "data event with image/png is rejected",
			inputContentType: "image/png",
			isBase64:         false,
			expectError:      true,
		},
		{
			name:             "data event with arbitrary type is rejected",
			inputContentType: "text/plain",
			isBase64:         false,
			expectError:      true,
		},
		{
			name:             "data_base64 event with empty content type is rejected",
			inputContentType: "",
			isBase64:         true,
			expectError:      true,
		},
		{
			name:                "data_base64 event with image/png passes",
			inputContentType:    "image/png",
			isBase64:            true,
			expectedContentType: "image/png",
		},
		{
			name:                "data_base64 event with image/jpeg passes",
			inputContentType:    "image/jpeg",
			isBase64:            true,
			expectedContentType: "image/jpeg",
		},
		{
			name:                "data_base64 event with application/pdf passes",
			inputContentType:    "application/pdf",
			isBase64:            true,
			expectedContentType: "application/pdf",
		},
		{
			name:                "data_base64 event with application/json passes",
			inputContentType:    "application/json",
			isBase64:            true,
			expectedContentType: "application/json",
		},
		{
			name:             "data_base64 event with non-whitelisted type is rejected",
			inputContentType: "text/plain",
			isBase64:         true,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hdr := &cloudevent.CloudEventHeader{DataContentType: tt.inputContentType}
			err := validateAndSetContentType(hdr, tt.isBase64)
			if tt.expectError {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrValidation)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expectedContentType, hdr.DataContentType)
		})
	}
}
