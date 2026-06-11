package compact

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/DIMO-Network/cloudevent"
	ceparquet "github.com/DIMO-Network/cloudevent/parquet"
	"github.com/DIMO-Network/din/internal/pqwrite"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog"
)

// compactedPrefix names compaction outputs. It must sort lexicographically
// before "ingest-" so compacted files always compare at-or-below any
// materializer cursor in their partition.
const compactedPrefix = "c1-"

// Config tunes the compactor. Zero values take the plan defaults.
type Config struct {
	// Root is the partition root to compact, normally pqwrite.RawPrefix.
	Root string
	// Interval is the planning cadence.
	Interval time.Duration
	// LookbackDays bounds which date partitions are scanned each cycle.
	LookbackDays int
	// MaxSourceSize excludes already-large files from compaction input.
	MaxSourceSize int64
	// MinFiles triggers compaction at this candidate count.
	MinFiles int
	// MinBytes triggers compaction at this combined candidate size.
	MinBytes int64
	// MaxOutputSize chunks merged output below this size estimate.
	MaxOutputSize int64
	// DeleteGrace delays source deletion after outputs land so in-flight
	// readers finish. Readers tolerate the duplicate window via dedup.
	DeleteGrace time.Duration
	// DecodedPrefix is the decoded layout root the dq materializer writes
	// under; the watermark cursor lives at <prefix>_state/watermark.json.
	// Must match dq's DECODED_PREFIX exactly.
	DecodedPrefix string
}

// watermarkKey locates the materializer cursor under the decoded prefix.
func (c *Config) watermarkKey() string {
	return c.DecodedPrefix + watermarkSuffix
}

func (c *Config) applyDefaults() {
	if c.Root == "" {
		c.Root = pqwrite.RawPrefix
	}
	if c.Interval == 0 {
		c.Interval = 15 * time.Minute
	}
	if c.LookbackDays == 0 {
		c.LookbackDays = 7
	}
	if c.MaxSourceSize == 0 {
		c.MaxSourceSize = 384 << 20
	}
	if c.MinFiles == 0 {
		c.MinFiles = 8
	}
	if c.MinBytes == 0 {
		c.MinBytes = 256 << 20
	}
	if c.MaxOutputSize == 0 {
		c.MaxOutputSize = 512 << 20
	}
	if c.DeleteGrace == 0 {
		c.DeleteGrace = 10 * time.Minute
	}
	if c.DecodedPrefix == "" {
		c.DecodedPrefix = DefaultDecodedPrefix
	}
	if !strings.HasSuffix(c.DecodedPrefix, "/") {
		c.DecodedPrefix += "/"
	}
	// A cycle that re-lists a partition while its sources await grace
	// deletion would re-merge them (harmless via dedup, but it churns S3
	// and manifests). Keep the planning cadence behind the grace window.
	if c.Interval <= c.DeleteGrace {
		c.Interval = c.DeleteGrace + 5*time.Minute
	}
}

// Compactor plans and executes partition merges.
type Compactor struct {
	cfg   Config
	store ObjectStore
	log   zerolog.Logger
	now   func() time.Time
	sleep func(context.Context, time.Duration)

	// deletes tracks in-flight grace-period source deletions so Run can
	// drain them on shutdown. Deletions left pending at a crash are
	// finished by the recovery pass (the manifest is already durable).
	deletes sync.WaitGroup
}

// New constructs a Compactor.
func New(cfg Config, store ObjectStore, log zerolog.Logger) *Compactor {
	cfg.applyDefaults()
	return &Compactor{
		cfg:   cfg,
		store: store,
		log:   log.With().Str("component", "compactor").Logger(),
		now:   time.Now,
		sleep: func(ctx context.Context, d time.Duration) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		},
	}
}

// Run executes Recover once, then compaction cycles until ctx is canceled.
func (c *Compactor) Run(ctx context.Context) error {
	if err := c.Recover(ctx); err != nil {
		return fmt.Errorf("compaction recovery: %w", err)
	}
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		if err := c.Cycle(ctx); err != nil {
			c.log.Error().Err(err).Msg("compaction cycle failed")
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			c.DrainDeletes()
			return nil
		}
	}
}

// DrainDeletes blocks until all in-flight grace-period deletions finish.
// Run calls it on shutdown; tests use it to observe post-grace state.
func (c *Compactor) DrainDeletes() {
	c.deletes.Wait()
}

// Cycle plans and executes one compaction pass across the lookback window.
func (c *Compactor) Cycle(ctx context.Context) error {
	watermark, err := LoadWatermark(ctx, c.store, c.cfg.watermarkKey())
	if err != nil {
		if errors.Is(err, ErrNoWatermark) {
			c.log.Info().Msg("no materializer watermark yet; skipping cycle")
			return nil
		}
		return err
	}

	partitions, err := c.listPartitions(ctx)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.compactPartition(ctx, partition, watermark); err != nil {
			c.log.Error().Err(err).Str("partition", partition).Msg("partition compaction failed")
		}
	}
	return nil
}

// listPartitions finds type=*/date=* prefixes within the lookback window by
// listing the root once and bucketing object keys.
func (c *Compactor) listPartitions(ctx context.Context) ([]string, error) {
	objects, err := c.store.List(ctx, c.cfg.Root+"/type=")
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", c.cfg.Root, err)
	}

	oldest := c.now().UTC().AddDate(0, 0, -c.cfg.LookbackDays).Format("2006-01-02")
	seen := map[string]bool{}
	var partitions []string
	for _, obj := range objects {
		partition, _, ok := splitPartitionKey(c.cfg.Root, obj.Key)
		if !ok || seen[partition] {
			continue
		}
		if date, ok := strings.CutPrefix(path.Base(partition), "date="); ok && date >= oldest {
			seen[partition] = true
			partitions = append(partitions, partition)
		}
	}
	sort.Strings(partitions)
	return partitions, nil
}

// compactPartition merges eligible small files in one partition.
func (c *Compactor) compactPartition(ctx context.Context, partition string, watermark Watermark) error {
	prefix := c.cfg.Root + "/" + partition + "/"
	objects, err := c.store.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("listing partition: %w", err)
	}

	var candidates []ObjectInfo
	var totalBytes int64
	for _, obj := range objects {
		name := path.Base(obj.Key)
		if !strings.HasSuffix(name, ".parquet") || obj.Size >= c.cfg.MaxSourceSize {
			continue
		}
		// Skip prior compaction outputs; re-merging them is pure write
		// amplification (dedup makes it harmless but never useful).
		if strings.HasPrefix(name, compactedPrefix) {
			continue
		}
		if !watermark.Covers(partition, obj.Key) {
			continue
		}
		candidates = append(candidates, obj)
		totalBytes += obj.Size
	}

	if len(candidates) < 2 {
		return nil
	}
	if len(candidates) < c.cfg.MinFiles && totalBytes < c.cfg.MinBytes && !c.partitionClosed(partition) {
		return nil
	}

	return c.merge(ctx, partition, candidates)
}

// partitionClosed reports whether the partition's date is in the past, in
// which case even small file sets converge to one file per day.
func (c *Compactor) partitionClosed(partition string) bool {
	date, ok := strings.CutPrefix(path.Base(partition), "date=")
	return ok && date < c.now().UTC().Format("2006-01-02")
}

// merge downloads candidates, merges + dedups rows, writes chunked c1-
// outputs, records a manifest, then grace-deletes the sources.
func (c *Compactor) merge(ctx context.Context, partition string, sources []ObjectInfo) error {
	var events []cloudevent.StoredEvent
	var sourceKeys []string
	var inputBytes int64
	for _, src := range sources {
		body, err := c.store.GetObject(ctx, src.Key)
		if err != nil {
			return fmt.Errorf("downloading %s: %w", src.Key, err)
		}
		decoded, err := ceparquet.Decode(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			c.log.Warn().Err(err).Str("key", src.Key).Msg("skipping undecodable source file")
			continue
		}
		events = append(events, decoded...)
		sourceKeys = append(sourceKeys, src.Key)
		inputBytes += src.Size
	}
	if len(sourceKeys) < 2 {
		return nil
	}

	events = dedupByKey(events)

	// Chunk so each output stays under MaxOutputSize, estimated from input
	// bytes (post-merge compression only shrinks it).
	chunks := 1
	if inputBytes > c.cfg.MaxOutputSize {
		chunks = int((inputBytes + c.cfg.MaxOutputSize - 1) / c.cfg.MaxOutputSize)
	}
	perChunk := (len(events) + chunks - 1) / chunks

	dir := c.cfg.Root + "/" + partition + "/"
	var outputs []string
	var bodies [][]byte
	for start := 0; start < len(events); start += perChunk {
		end := min(start+perChunk, len(events))
		now := c.now()
		key := fmt.Sprintf("%s%s%013d-%s.parquet", dir, compactedPrefix, now.UnixMilli(), ulid.MustNew(ulid.Timestamp(now), rand.Reader))
		body, err := pqwrite.Encode(events[start:end], key)
		if err != nil {
			return fmt.Errorf("encoding compacted chunk: %w", err)
		}
		outputs = append(outputs, key)
		bodies = append(bodies, body)
	}

	for i, key := range outputs {
		if err := c.store.PutObject(ctx, key, bodies[i]); err != nil {
			return fmt.Errorf("uploading compacted file %s: %w", key, err)
		}
	}

	manifestID := ulid.MustNew(ulid.Timestamp(c.now()), rand.Reader).String()
	m := Manifest{Sources: sourceKeys, Outputs: outputs, CreatedAt: c.now().UTC()}
	if err := writeManifest(ctx, c.store, c.cfg.Root, manifestID, m); err != nil {
		return err
	}

	// Grace-delete asynchronously so one merge's 10-minute window does not
	// serialize the rest of the cycle. If we shut down or crash first, the
	// durable manifest lets Recover finish the deletes.
	c.deletes.Add(1)
	go func() {
		defer c.deletes.Done()
		c.sleep(ctx, c.cfg.DeleteGrace)
		if ctx.Err() != nil {
			return // recovery will finish this unit
		}
		if err := c.finishManifest(ctx, manifestID, m); err != nil {
			c.log.Error().Err(err).Str("manifest", manifestID).Msg("grace delete failed; recovery will retry")
		}
	}()

	c.log.Info().Str("partition", partition).Int("sources", len(sourceKeys)).
		Int("outputs", len(outputs)).Int("rows", len(events)).Msg("partition compacted")
	return nil
}

// finishManifest deletes sources then the manifest itself.
func (c *Compactor) finishManifest(ctx context.Context, id string, m Manifest) error {
	for _, src := range m.Sources {
		if err := c.store.DeleteObject(ctx, src); err != nil {
			return fmt.Errorf("deleting source %s: %w", src, err)
		}
	}
	if err := c.store.DeleteObject(ctx, manifestKey(c.cfg.Root, id)); err != nil {
		return fmt.Errorf("deleting manifest %s: %w", id, err)
	}
	return nil
}

// Recover reconciles leftover manifests from a crash: complete units finish
// their deletes; incomplete units roll back partial outputs.
func (c *Compactor) Recover(ctx context.Context) error {
	manifests, err := listManifests(ctx, c.store, c.cfg.Root)
	if err != nil {
		return err
	}

	for id, m := range manifests {
		complete := true
		for _, out := range m.Outputs {
			// Existence check via List: no full download, and a transient
			// store error aborts recovery instead of masquerading as a
			// missing output and rolling back a finished compaction.
			objs, err := c.store.List(ctx, out)
			if err != nil {
				return fmt.Errorf("recovery: checking output %s: %w", out, err)
			}
			found := false
			for _, obj := range objs {
				if obj.Key == out {
					found = true
					break
				}
			}
			if !found {
				complete = false
				break
			}
		}

		if complete {
			c.log.Info().Str("manifest", id).Msg("recovery: finishing completed compaction")
			if err := c.finishManifest(ctx, id, m); err != nil {
				return err
			}
			continue
		}

		c.log.Warn().Str("manifest", id).Msg("recovery: rolling back partial compaction")
		for _, out := range m.Outputs {
			if err := c.store.DeleteObject(ctx, out); err != nil {
				c.log.Warn().Err(err).Str("key", out).Msg("rollback delete failed (may not exist)")
			}
		}
		if err := c.store.DeleteObject(ctx, manifestKey(c.cfg.Root, id)); err != nil {
			return fmt.Errorf("deleting manifest %s during rollback: %w", id, err)
		}
	}
	return nil
}

// splitPartitionKey extracts "type=T/date=D" and the file name from a full
// object key under root. Returns ok=false for keys outside partitions
// (e.g. the _compaction prefix).
func splitPartitionKey(root, key string) (partition, file string, ok bool) {
	rest, found := strings.CutPrefix(key, root+"/")
	if !found || !strings.HasPrefix(rest, "type=") {
		return "", "", false
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || !strings.HasPrefix(parts[1], "date=") {
		return "", "", false
	}
	return parts[0] + "/" + parts[1], parts[2], true
}

// dedupByKey removes rows sharing a header uniqueness key, keeping the
// first occurrence. This is where at-least-once redelivery duplicates die.
func dedupByKey(events []cloudevent.StoredEvent) []cloudevent.StoredEvent {
	seen := make(map[string]struct{}, len(events))
	out := events[:0]
	for _, ev := range events {
		key := ev.Key()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ev)
	}
	return out
}
