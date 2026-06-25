package lake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnInit_AllPooledConnsAreUTC proves the connector's per-connection init
// runs on every pooled connection, not just the one bootstrap() used. The
// raw_events partition key is day("time") over a TIMESTAMP WITH TIME ZONE
// column, so a writer/recycled conn left at the process TimeZone would file
// near-midnight-UTC events under the wrong (type, day) partition. Hold several
// conns open at once to force distinct physical connections and assert each is
// pinned to UTC.
func TestConnInit_AllPooledConnsAreUTC(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()
	db := l.DB()

	const n = 4
	conns := make([]*sql.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := db.Conn(ctx) // each held-open checkout forces a fresh physical conn
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for i, c := range conns {
		var tz string
		require.NoError(t, c.QueryRowContext(ctx, "SELECT current_setting('TimeZone')").Scan(&tz))
		assert.Equal(t, "UTC", tz, "pooled connection %d is not pinned to UTC (connInit did not run)", i)
		require.NoError(t, c.Close())
	}
}

// openTestLake opens a Lake on a DuckDB-file catalog in a temp dir — no
// Postgres or S3 needed. Returns the lake and its DATA_PATH.
func openTestLake(t *testing.T) (*Lake, string) {
	t.Helper()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	l, err := Open(context.Background(), Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   dataPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return l, dataPath
}

func testEvent(id, ceType, subject string, ts time.Time) cloudevent.StoredEvent {
	return cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				SpecVersion: cloudevent.SpecVersion,
				Type:        ceType,
				Subject:     subject,
				Source:      "0xConn",
				Producer:    "0xDev",
				ID:          id,
				Time:        ts,
			},
			Data: json.RawMessage(`{"v":1}`),
		},
	}
}

func TestOpen_BootstrapIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   filepath.Join(dir, "data"),
	}
	ctx := context.Background()

	l, err := Open(ctx, cfg)
	require.NoError(t, err)

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ?`,
		RawTable).Scan(&n))
	assert.Equal(t, 1, n)
	require.NoError(t, l.Close())

	// Reopen: bootstrap must be a no-op, not an error or a new table.
	l2, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer l2.Close() //nolint:errcheck
	var snapshotsBefore int
	require.NoError(t, l2.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.snapshots()").Scan(&snapshotsBefore))
	require.NoError(t, l2.Close())
	l3, err := Open(ctx, cfg)
	require.NoError(t, err)
	defer l3.Close() //nolint:errcheck
	var snapshotsAfter int
	require.NoError(t, l3.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.snapshots()").Scan(&snapshotsAfter))
	assert.Equal(t, snapshotsBefore, snapshotsAfter, "reboot must not mint snapshots")
}

func TestWriter_RoundTrip(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 8, 10, 30, 0, 0, time.UTC)
	inline := testEvent("e-inline", "dimo.status", "did:erc721:137:0xA:1", ts)
	blob := testEvent("e-blob", "dimo.attestation", "did:erc721:137:0xB:2", ts)
	blob.Data = nil
	blob.DataBase64 = "SGVsbG8gV29ybGQ="
	blob.DataIndexKey = "cloudevent/blobs/xyz"
	empty := testEvent("e-empty", "dimo.status", "did:erc721:137:0xC:3", ts)
	empty.Data = nil

	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{inline, blob, empty}))

	rows, err := l.DB().QueryContext(ctx, `SELECT id, subject, "time", type, extras,
		data, data_base64, data_index_key FROM lake.raw_events ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	type got struct {
		id, subject, ceType, extras string
		ts                          time.Time
		data, dataIndexKey          *string
		dataBase64                  []byte
	}
	var all []got
	for rows.Next() {
		var g got
		require.NoError(t, rows.Scan(&g.id, &g.subject, &g.ts, &g.ceType, &g.extras,
			&g.data, &g.dataBase64, &g.dataIndexKey))
		all = append(all, g)
	}
	require.NoError(t, rows.Err())
	require.Len(t, all, 3)

	require.Equal(t, "e-blob", all[0].id)
	assert.Nil(t, all[0].data)
	assert.Equal(t, []byte("SGVsbG8gV29ybGQ="), all[0].dataBase64)
	require.NotNil(t, all[0].dataIndexKey)
	assert.Equal(t, "cloudevent/blobs/xyz", *all[0].dataIndexKey)

	require.Equal(t, "e-empty", all[1].id)
	assert.Nil(t, all[1].data)
	assert.Nil(t, all[1].dataBase64)
	assert.Nil(t, all[1].dataIndexKey)

	require.Equal(t, "e-inline", all[2].id)
	require.NotNil(t, all[2].data)
	assert.JSONEq(t, `{"v":1}`, *all[2].data)
	assert.Nil(t, all[2].dataBase64)
	assert.Equal(t, "{}", all[2].extras)
	assert.True(t, all[2].ts.Equal(ts), "got %v want %v", all[2].ts, ts)
	assert.Equal(t, "did:erc721:137:0xA:1", all[2].subject)
	assert.Equal(t, "dimo.status", all[2].ceType)
}

func TestWriter_OneSnapshotPerBundle(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	var before int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.snapshots()").Scan(&before))

	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	for i := range 3 {
		bundle := []cloudevent.StoredEvent{
			testEvent(fmt.Sprintf("a%d", i), "dimo.status", "did:1", ts),
			testEvent(fmt.Sprintf("b%d", i), "dimo.status", "did:2", ts),
		}
		require.NoError(t, w.WriteBundle(ctx, bundle))
	}

	var after int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.snapshots()").Scan(&after))
	assert.Equal(t, 3, after-before, "one snapshot per bundle")

	// Empty bundles are free.
	require.NoError(t, w.WriteBundle(ctx, nil))
	var afterEmpty int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.snapshots()").Scan(&afterEmpty))
	assert.Equal(t, after, afterEmpty)
}

func TestWriter_PartitionedDataLayout(t *testing.T) {
	t.Parallel()
	l, dataPath := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	// Big enough to clear DuckLake's data-inlining threshold — tiny
	// bundles live as rows in the catalog until maintenance flushes them.
	day1 := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range 20 {
		bundle = append(bundle,
			testEvent(fmt.Sprintf("s1-%d", i), "dimo.status", "did:1", day1),
			testEvent(fmt.Sprintf("s2-%d", i), "dimo.status", "did:1", day2),
			testEvent(fmt.Sprintf("f-%d", i), "dimo.fingerprint", "did:2", day2),
		)
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	// One INSERT splits across (type, day) partitions on disk.
	var partitioned []string
	require.NoError(t, filepath.WalkDir(dataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			rel, _ := filepath.Rel(dataPath, path)
			partitioned = append(partitioned, rel)
		}
		return nil
	}))
	require.Len(t, partitioned, 3, "expected one file per (type, day) partition: %v", partitioned)

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM lake.raw_events WHERE type = 'dimo.status' AND "time"::DATE = DATE '2026-06-09'`).Scan(&n))
	assert.Equal(t, 20, n)

	// Written files keep the old pqwrite bundles' pruning traits: zstd
	// compression and a usable subject bloom filter.
	file := filepath.Join(dataPath, partitioned[0])
	var compression string
	var hasBloom bool
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT compression, bloom_filter_offset IS NOT NULL FROM parquet_metadata(%s)
		 WHERE path_in_schema = 'subject' AND row_group_id = 0`, sqlString(file))).
		Scan(&compression, &hasBloom))
	assert.Equal(t, "ZSTD", compression)
	assert.True(t, hasBloom, "subject column must carry a bloom filter")
}

func TestWriter_TinyBundleInlines(t *testing.T) {
	t.Parallel()
	l, dataPath := openTestLake(t)
	ctx := context.Background()

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	require.NoError(t, w.WriteBundle(ctx, []cloudevent.StoredEvent{
		testEvent("e1", "dimo.status", "did:1", ts),
	}))

	// Below the inlining threshold no Parquet file is written; the row
	// lives in the catalog but is fully queryable.
	var files int
	_ = filepath.WalkDir(dataPath, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			files++
		}
		return nil
	})
	assert.Zero(t, files, "tiny bundle should inline into the catalog")

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 1, n)
}

func TestWriter_ConcurrentBundles(t *testing.T) {
	t.Parallel()
	l, _ := openTestLake(t)
	ctx := context.Background()

	w1, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w1.Close() //nolint:errcheck
	w2, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w2.Close() //nolint:errcheck

	ts := time.Date(2026, 6, 8, 10, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, w := range []*Writer{w1, w2} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 5 {
				bundle := []cloudevent.StoredEvent{
					testEvent(fmt.Sprintf("w%d-%d", i, j), "dimo.status", fmt.Sprintf("did:%d", i), ts),
				}
				if err := w.WriteBundle(ctx, bundle); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	var n int
	require.NoError(t, l.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 10, n)
}
