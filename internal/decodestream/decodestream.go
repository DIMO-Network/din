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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/din/internal/convert"
	"github.com/DIMO-Network/din/internal/stream"
	"github.com/DIMO-Network/model-garage/pkg/modules"
	mgconvert "github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
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

// EnsureStreams provisions the decoded streams with the same shape
// vehicle-triggers-api expects.
func (b *Bridge) EnsureStreams(ctx context.Context) error {
	for _, sc := range []jetstream.StreamConfig{
		{
			Name:      SignalsStreamName,
			Subjects:  []string{signalSubjectPrefix + ".>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    24 * time.Hour,
			Replicas:  b.cfg.Replicas,
		},
		{
			Name:      EventsStreamName,
			Subjects:  []string{eventSubjectPrefix + ".>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    24 * time.Hour,
			Replicas:  b.cfg.Replicas,
		},
	} {
		if _, err := b.js.CreateOrUpdateStream(ctx, sc); err != nil {
			return fmt.Errorf("creating %s stream: %w", sc.Name, err)
		}
	}
	return nil
}

// Run consumes INGEST_RAW status/event messages until ctx is canceled.
func (b *Bridge) Run(ctx context.Context) error {
	raw, err := b.js.Stream(ctx, stream.StreamName)
	if err != nil {
		return fmt.Errorf("opening %s: %w", stream.StreamName, err)
	}
	cons, err := raw.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Minute,
		FilterSubjects: []string{
			stream.SubjectFilterForType(cloudevent.TypeStatus),
			stream.SubjectFilterForType(cloudevent.TypeEvents),
		},
	})
	if err != nil {
		return fmt.Errorf("creating %s consumer: %w", consumerName, err)
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

// handle decodes one raw message and publishes its signals/events. The raw
// message is always acked: decode failures are terminal for this payload
// and the raw bytes remain in parquet for parse-on-read recovery.
func (b *Bridge) handle(ctx context.Context, msg jetstream.Msg) {
	defer func() {
		if err := msg.Ack(); err != nil {
			b.log.Warn().Err(err).Msg("acking raw message failed")
		}
	}()

	event, err := stream.ParseMsg(msg.Headers(), msg.Data())
	if err != nil {
		b.log.Error().Err(err).Str("subject", msg.Subject()).Msg("undecodable raw message")
		return
	}

	switch event.Type {
	case cloudevent.TypeStatus:
		if b.isVehicleSignalMessage(&event.RawEvent) {
			b.publishSignals(ctx, &event.RawEvent)
		}
	case cloudevent.TypeEvents:
		b.publishEvents(ctx, &event.RawEvent)
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
// name so per-name subject filters stay exact.
func (b *Bridge) publishSignals(ctx context.Context, rawEvent *cloudevent.RawEvent) {
	signals, err := modules.ConvertToSignals(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		var convertErr *mgconvert.ConversionError
		if errors.As(err, &convertErr) {
			signals = convertErr.DecodedSignals
		}
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal conversion errors")
	}
	if len(signals) == 0 {
		return
	}

	signals, pruneErr := pruneFutureAndDuplicateSignals(signals)
	signals, locErr := handleCoordinates(signals)
	if err := errors.Join(pruneErr, locErr); err != nil {
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("signal pruning errors")
	}
	if len(signals) == 0 {
		return
	}

	header := decodedHeader(rawEvent, cloudevent.TypeSignals)
	for name, group := range groupSignalsByName(signals) {
		signalCE := vss.PackSignals(header, group)
		b.publishJSON(ctx, signalSubjectPrefix+"."+sanitize(name), signalCE)
	}
}

func (b *Bridge) publishEvents(ctx context.Context, rawEvent *cloudevent.RawEvent) {
	events, err := modules.ConvertToEvents(ctx, rawEvent.Source, *rawEvent)
	if err != nil {
		b.log.Warn().Err(err).Str("source", rawEvent.Source).Msg("event conversion errors")
	}
	if len(events) == 0 {
		return
	}

	header := decodedHeader(rawEvent, cloudevent.TypeEvents)
	byName := map[string][]vss.Event{}
	for _, ev := range events {
		byName[ev.Data.Name] = append(byName[ev.Data.Name], ev)
	}
	for name, group := range byName {
		eventCE := vss.PackEvents(header, group)
		b.publishJSON(ctx, eventSubjectPrefix+"."+sanitize(name), eventCE)
	}
}

func (b *Bridge) publishJSON(ctx context.Context, subject string, payload any) {
	if _, err := b.js.Publish(ctx, subject, mustJSON(payload)); err != nil {
		b.log.Error().Err(err).Str("subject", subject).Msg("publishing decoded message failed")
	}
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

// sanitize matches vehicle-triggers-api's subject sanitizer.
func sanitize(s string) string {
	if s == "" {
		return "_"
	}
	r := strings.NewReplacer(" ", "_", ".", "_", "*", "_", ">", "_", "\t", "_", "\n", "_", "\r", "_")
	return r.Replace(s)
}

func mustJSON(v any) []byte {
	body, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshaling decoded payload: %v", err))
	}
	return body
}
