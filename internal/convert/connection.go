package convert

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"
)

// futureWarnInterval is how often a future-timestamp warning is logged per
// producer (rate-limited the same way DIS did with ratedlogger).
const futureWarnInterval = time.Hour

// maxFutureWarnProducers bounds the warn-dedup map (keys are
// device-supplied producer strings).
const maxFutureWarnProducers = 100_000

// Converter converts raw connection payloads into validated, canonicalized
// CloudEvents using the model-garage module registry.
type Converter struct {
	logger zerolog.Logger

	mu             sync.Mutex
	lastFutureWarn map[string]time.Time
}

// NewConverter registers the conversion modules for cfg and returns a
// Converter ready to process connection payloads.
func NewConverter(logger zerolog.Logger, cfg Config) *Converter {
	RegisterModules(cfg)
	return &Converter{
		logger:         logger,
		lastFutureWarn: map[string]time.Time{},
	}
}

// Convert converts a raw connection payload from sourceAddr into one or more
// CloudEvents. A module may fan a single payload out into multiple headers; in
// that case every returned event shares the same payload data. All returned
// events are validated and have their Subject, Producer, and Source rewritten
// to canonical form.
func (c *Converter) Convert(ctx context.Context, sourceAddr string, body []byte) ([]cloudevent.RawEvent, error) {
	hdrs, eventData, err := modules.ConvertToCloudEvents(ctx, sourceAddr, body)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to convert to cloud event: %w", ErrValidation, err)
	}
	if len(hdrs) == 0 {
		return nil, fmt.Errorf("%w: no cloud events headers returned", ErrValidation)
	}
	if len(eventData) == 0 {
		// If the module chooses not to return data, use the original message
		eventData = body
	}

	events := make([]cloudevent.RawEvent, len(hdrs))
	defaultID := ksuid.New().String()
	// set defaults for each header, then create an event for each header
	for i := range hdrs {
		hdr := &hdrs[i]
		if IsFutureTimestamp(hdr.Time) {
			c.warnFutureTimestamp(hdr)
		}
		if err := ValidateHeadersAndSetDefaults(hdr, sourceAddr, defaultID, false); err != nil {
			return nil, fmt.Errorf("invalid cloud event header string: %w", err)
		}
		if !isValidConnectionType(hdr) {
			return nil, fmt.Errorf("%w: unsupported cloud event type: %s", ErrValidation, hdr.Type)
		}
		if !canonicalizeConnectionHeader(hdr, c.logger) {
			c.logger.Warn().Msgf("invalid cloud event header for header=%+v", hdr)
		}
		events[i] = cloudevent.RawEvent{
			CloudEventHeader: *hdr,
			Data:             eventData,
		}
	}

	return events, nil
}

// warnFutureTimestamp logs a future-timestamp warning at most once per
// futureWarnInterval per producer.
func (c *Converter) warnFutureTimestamp(hdr *cloudevent.CloudEventHeader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if last, ok := c.lastFutureWarn[hdr.Producer]; ok && time.Since(last) < futureWarnInterval {
		return
	}
	// Bound the dedup map: producer strings come from device payloads, so
	// cardinality is attacker-influenced. Resetting just re-allows one
	// warning per producer — harmless.
	if len(c.lastFutureWarn) >= maxFutureWarnProducers {
		clear(c.lastFutureWarn)
	}
	c.lastFutureWarn[hdr.Producer] = time.Now()
	c.logger.Warn().Msgf("Cloud event time is in the future: now() = %v is before event.time = %v \n %+v", time.Now(), hdr.Time, hdr)
}

// canonicalizeConnectionHeader validates a connection cloud event header and
// rewrites Subject, Producer, and Source in place so that any contract or
// account address is in EIP-55 checksum form. Lowercased / mixed-case
// addresses are accepted on input but normalized before being passed
// downstream (ClickHouse, Kafka, Parquet). Legacy `did:nft:` values are also
// rewritten to their canonical `did:erc721:` form.
func canonicalizeConnectionHeader(eventHdr *cloudevent.CloudEventHeader, logger zerolog.Logger) bool {
	if did, err := cloudevent.DecodeERC721DID(eventHdr.Subject); err == nil {
		eventHdr.Subject = did.String()
	} else {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Subject)
		if err != nil {
			return false
		}
		eventHdr.Subject = did.String()
		logger.Debug().Msgf("Cloud event header subject for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	if did, err := cloudevent.DecodeERC721DID(eventHdr.Producer); err == nil {
		eventHdr.Producer = did.String()
	} else {
		did, err := cloudevent.DecodeLegacyNFTDID(eventHdr.Producer)
		if err != nil {
			return false
		}
		eventHdr.Producer = did.String()
		logger.Debug().Msgf("Cloud event header producer for source %s is a legacy NFT DID: %v", eventHdr.Source, eventHdr)
	}

	if !common.IsHexAddress(eventHdr.Source) {
		return false
	}
	eventHdr.Source = common.HexToAddress(eventHdr.Source).Hex()
	return true
}

func isValidConnectionType(eventHdr *cloudevent.CloudEventHeader) bool {
	return eventHdr.Type == cloudevent.TypeStatus || eventHdr.Type == cloudevent.TypeFingerprint || eventHdr.Type == cloudevent.TypeEvents || eventHdr.Type == cloudevent.TypeSignals || eventHdr.Type == cloudevent.TypeRawStatus
}
