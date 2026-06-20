// Package decodestream bridges the raw INGEST_RAW stream to the decoded
// per-signal-name subjects vehicle-triggers-api consumes today
// (dimo.signals.<name> / dimo.events.<name> with packed SignalCloudEvent /
// EventCloudEvent payloads). It exists so triggers stay untouched at
// cutover; once triggers consume raw directly this module is deleted.
// Decoding here is off the HTTP hot path — it is just another consumer.
package decodestream

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/stream"
	mgconvert "github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const (
	// SignalsStreamName / EventsStreamName match vehicle-triggers-api's
	// stream provisioning (settings SignalsStream / EventsStream).
	SignalsStreamName = "DIMO_SIGNALS"
	EventsStreamName  = "DIMO_EVENTS"

	signalSubjectPrefix = "dimo.signals"
	eventSubjectPrefix  = "dimo.events"

	consumerName = "decodestream"

	pruneSignalName = "___prune"
)

var (
	errFutureTimestamp = errors.New("future timestamp")
	errLatLongMismatch = errors.New("latitude and longitude mismatch")
	pruneSignal        = vss.Signal{Data: vss.SignalData{Name: pruneSignalName}}
)

// Config wires the bridge.
type Config struct {
	// ChainID and VehicleNFTAddress gate which raw events are vehicle
	// signal messages, mirroring dis signalconvert.
	ChainID           uint64
	VehicleNFTAddress common.Address
	// Replicas for the decoded streams.
	Replicas int
	// StreamPartitions is the WAL partition count to consume (must match
	// the publisher's NATS_STREAM_PARTITIONS).
	StreamPartitions int
}

// Bridge consumes raw status/event messages and republishes decoded
// signals/events on triggers-compatible subjects.
type Bridge struct {
	cfg Config
	js  jetstream.JetStream
	log zerolog.Logger
}

// New constructs a Bridge.
func New(cfg Config, js jetstream.JetStream, log zerolog.Logger) *Bridge {
	if cfg.Replicas == 0 {
		cfg.Replicas = 1
	}
	return &Bridge{cfg: cfg, js: js, log: log.With().Str("component", "decodestream").Logger()}
}

// EnsureStreams provisions the decoded streams BYTE-COMPATIBLE with
// vehicle-triggers-api's own provisioning (internal/nats/provision.go):
// both sides CreateOrUpdateStream the same names, so any config drift
// makes the stream definition flap on every alternating restart.
func (b *Bridge) EnsureStreams(ctx context.Context) error {
	for _, sc := range []jetstream.StreamConfig{
		{
			Name:        SignalsStreamName,
			Subjects:    []string{signalSubjectPrefix + ".>"},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			Discard:     jetstream.DiscardOld,
			MaxAge:      24 * time.Hour,
			MaxBytes:    100 << 30, // vta SIGNALS_MAX_BYTES default
			Replicas:    b.cfg.Replicas,
			Description: "DIMO vehicle signal telemetry",
		},
		{
			Name:        EventsStreamName,
			Subjects:    []string{eventSubjectPrefix + ".>"},
			Storage:     jetstream.FileStorage,
			Retention:   jetstream.LimitsPolicy,
			Discard:     jetstream.DiscardOld,
			MaxAge:      24 * time.Hour,
			MaxBytes:    10 << 30, // vta EVENTS_MAX_BYTES default
			Replicas:    b.cfg.Replicas,
			Description: "DIMO vehicle events",
		},
	} {
		if _, err := b.js.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("creating %s stream: %w", sc.Name, err)
		}
	}
	return nil
}

// Run consumes status/event messages from every WAL partition until ctx is
// canceled.
func (b *Bridge) Run(ctx context.Context) error {
	partitions := b.cfg.StreamPartitions
	if partitions <= 0 {
		partitions = 1
	}
	g, gctx := errgroup.WithContext(ctx)
	for i := range partitions {
		g.Go(func() error { return b.runPartition(gctx, i, partitions) })
	}
	return g.Wait()
}

func (b *Bridge) runPartition(ctx context.Context, partition, partitions int) error {
	streamName := stream.StreamNameFor(partition, partitions)
	raw, err := b.js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("opening %s: %w", streamName, err)
	}
	durable := consumerName
	if partitions > 1 {
		durable = fmt.Sprintf("%s-p%03d", consumerName, partition)
	}
	// Filters match any partition suffix: type/subject tokens come first.
	cons, err := raw.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Minute,
		FilterSubjects: []string{
			stream.SubjectFilterForType(cloudevent.TypeStatus),
			stream.SubjectFilterForType(cloudevent.TypeEvents),
		},
	})
	if err != nil {
		return fmt.Errorf("creating %s consumer: %w", durable, err)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		batch, err := cons.Fetch(500, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("fetching raw messages: %w", err)
		}
		for msg := range batch.Messages() {
			b.handle(ctx, msg)
		}
	}
}

// handle decodes one raw message and publishes its signals/events.
// Conversion failures are terminal for the payload (raw bytes remain in
// parquet for parse-on-read recovery) and ack. Publish failures are
// transient — the message is Nak'd so JetStream redelivers instead of
// silently dropping decoded signals on the floor.
func (b *Bridge) handle(ctx context.Context, msg jetstream.Msg) {
	event, err := stream.ParseMsg(msg.Headers(), msg.Data())
	if err != nil {
		b.log.Error().Err(err).Str("subject", msg.Subject()).Msg("undecodable raw message")
		b.ack(msg)
		return
	}

	var pubErr error
	switch event.Type {
	case cloudevent.TypeStatus:
		if b.isVehicleSignalMessage(&event.RawEvent) {
			pubErr = b.publishSignals(ctx, &event.RawEvent)
		}
	case cloudevent.TypeEvents:
		pubErr = b.publishEvents(ctx, &event.RawEvent)
	}

	if pubErr != nil {
		b.log.Error().Err(pubErr).Str("subject", msg.Subject()).Msg("publishing decoded message failed; nak for redelivery")
		if err := msg.Nak(); err != nil {
			b.log.Warn().Err(err).Msg("nak failed")
		}
		return
	}
	b.ack(msg)
}

func (b *Bridge) ack(msg jetstream.Msg) {
	if err := msg.Ack(); err != nil {
		b.log.Warn().Err(err).Msg("acking raw message failed")
	}
}

func (b *Bridge) isVehicleSignalMessage(rawEvent *cloudevent.RawEvent) bool {
	did, err := cloudevent.DecodeERC721DID(rawEvent.Subject)
	if err != nil {
		return false
	}
	return did.ChainID == b.cfg.ChainID && did.ContractAddress.Cmp(b.cfg.VehicleNFTAddress) == 0
}

// publishSignals mirrors dis signalconvert: convert, salvage partial
// decodes, prune future/duplicate signals, merge coordinate pairs into
// location signals, then publish one packed SignalCloudEvent per signal
// name so per-name subject filters stay exact. The returned error covers
// only publish failures — conversion problems are logged and final.
func (b *Bridge) publishSignals(ctx context.Context, rawEvent *cloudevent.RawEvent) error {
	signals, err := modules.ConvertToSignals(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		var convertErr *mgconvert.ConversionError
		if errors.As(err, &convertErr) {
			signals = convertErr.DecodedSignals
		}
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal conversion errors")
	}
	if len(signals) == 0 {
		return nil
	}

	signals, pruneErr := pruneFutureAndDuplicateSignals(signals)
	signals, locErr := handleCoordinates(signals)
	if err := errors.Join(pruneErr, locErr); err != nil {
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal pruning errors")
	}
	if len(signals) == 0 {
		return nil
	}

	header := decodedHeader(rawEvent, cloudevent.TypeSignals)
	var futures []jetstream.PubAckFuture
	var pubErrs error
	for name, group := range groupSignalsByName(signals) {
		signalCE := vss.PackSignals(header, group)
		fut, err := b.publishJSONAsync(signalSubjectPrefix+"."+sanitize(name), signalCE, dedupID(rawEvent, name))
		if err != nil {
			pubErrs = errors.Join(pubErrs, err)
			continue
		}
		futures = append(futures, fut)
	}
	return errors.Join(pubErrs, b.awaitFutures(ctx, futures))
}

func (b *Bridge) publishEvents(ctx context.Context, rawEvent *cloudevent.RawEvent) error {
	events, err := modules.ConvertToEvents(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		// Salvage partial decodes exactly like dis eventconvert did.
		var convertErr *mgconvert.ConversionError
		if errors.As(err, &convertErr) {
			events = convertErr.DecodedEvents
		}
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("event conversion errors")
	}
	if len(events) == 0 {
		return nil
	}

	header := decodedHeader(rawEvent, cloudevent.TypeEvents)
	byName := map[string][]vss.Event{}
	for _, ev := range events {
		byName[ev.Data.Name] = append(byName[ev.Data.Name], ev)
	}
	var futures []jetstream.PubAckFuture
	var pubErrs error
	for name, group := range byName {
		eventCE := vss.PackEvents(header, group)
		fut, err := b.publishJSONAsync(eventSubjectPrefix+"."+sanitize(name), eventCE, dedupID(rawEvent, name))
		if err != nil {
			pubErrs = errors.Join(pubErrs, err)
			continue
		}
		futures = append(futures, fut)
	}
	return errors.Join(pubErrs, b.awaitFutures(ctx, futures))
}

// publishJSONAsync submits one publish without waiting for the ack; a raw
// status event fans out to one publish per signal name, so awaiting each
// ack serially would make per-name JetStream round-trips the bridge's
// throughput ceiling. The Nats-Msg-Id header makes redelivery replays
// (Nak after a partial publish failure) collapse inside the stream's
// duplicate window instead of double-delivering to vehicle-triggers.
func (b *Bridge) publishJSONAsync(subject string, payload any, dedup string) (jetstream.PubAckFuture, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling decoded payload for %s: %w", subject, err)
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{nats.MsgIdHdr: []string{dedup}},
	}
	fut, err := b.js.PublishMsgAsync(msg)
	if err != nil {
		return nil, fmt.Errorf("publishing to %s: %w", subject, err)
	}
	return fut, nil
}

// dedupID is deterministic per (raw event, decoded name): replays of the
// same raw message produce the same id.
func dedupID(rawEvent *cloudevent.RawEvent, name string) string {
	sum := sha256.Sum256([]byte(rawEvent.Key() + "|" + name))
	return hex.EncodeToString(sum[:16])
}

// awaitFutures waits for every pending ack; any failure fails the message
// so JetStream redelivers it (publishes are idempotent per subject+body).
func (b *Bridge) awaitFutures(ctx context.Context, futures []jetstream.PubAckFuture) error {
	var errs error
	for _, fut := range futures {
		select {
		case <-fut.Ok():
		case err := <-fut.Err():
			errs = errors.Join(errs, fmt.Errorf("async publish: %w", err))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errs
}

func decodedHeader(rawEvent *cloudevent.RawEvent, ceType string) cloudevent.CloudEventHeader {
	return cloudevent.CloudEventHeader{
		SpecVersion: rawEvent.SpecVersion,
		Subject:     rawEvent.Subject,
		Source:      rawEvent.Source,
		Producer:    rawEvent.Producer,
		ID:          rawEvent.ID,
		Time:        rawEvent.Time,
		Type:        ceType,
		DataVersion: rawEvent.DataVersion,
	}
}

func groupSignalsByName(signals []vss.Signal) map[string][]vss.Signal {
	byName := make(map[string][]vss.Signal)
	for _, sig := range signals {
		byName[sig.Data.Name] = append(byName[sig.Data.Name], sig)
	}
	return byName
}

// pruneFutureAndDuplicateSignals is ported from dis signalconvert.
func pruneFutureAndDuplicateSignals(signals []vss.Signal) ([]vss.Signal, error) {
	var errs error
	slices.SortFunc(signals, func(a, b vss.Signal) int {
		return cmp.Or(a.Data.Timestamp.Compare(b.Data.Timestamp), cmp.Compare(a.Data.Name, b.Data.Name))
	})
	for i := range signals {
		signal := &signals[i]
		if convert.IsFutureTimestamp(signal.Data.Timestamp) {
			errs = errors.Join(errs, fmt.Errorf("%w, signal '%s' has timestamp: %v",
				errFutureTimestamp, signal.Data.Name, signal.Data.Timestamp))
			signals[i] = pruneSignal
			continue
		}
		if i < len(signals)-1 && signalEqual(signals[i], signals[i+1]) {
			signals[i] = pruneSignal
		}
	}

	var pruned []vss.Signal
	for _, signal := range signals {
		if signal.Data.Name != pruneSignalName {
			pruned = append(pruned, signal)
		}
	}
	return pruned, errs
}

func signalEqual(a, b vss.Signal) bool {
	return a.Data.Name == b.Data.Name && a.Data.Timestamp.Equal(b.Data.Timestamp)
}

// subjectSanitizer matches vehicle-triggers-api's subject sanitizer. Hoisted to
// a package var: building the replacer's trie on every signal name was wasted
// work in the per-event fan-out (SR-20).
var subjectSanitizer = strings.NewReplacer(" ", "_", ".", "_", "*", "_", ">", "_", "\t", "_", "\n", "_", "\r", "_")

// sanitize matches vehicle-triggers-api's subject sanitizer.
func sanitize(s string) string {
	if s == "" {
		return "_"
	}
	return subjectSanitizer.Replace(s)
}
