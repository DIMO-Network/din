package lake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeAndInspect opens a lake at the given parquet_version, writes a sizeable
// single-day, single-type bundle (one (type, day) partition → one file), and
// returns the file's format_version, subject-column compression, the "time"
// column encodings, and total parquet bytes.
func writeAndInspect(t *testing.T, version string) (formatVersion, compression, timeEncodings string, totalBytes int64) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	l, err := Open(ctx, Config{
		CatalogDSN:     filepath.Join(dir, "meta.ducklake"),
		DataPath:       dataPath,
		ParquetVersion: version,
	})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range 5000 {
		// Monotonic time + many subjects: exercises delta/dictionary encodings.
		bundle = append(bundle, testEvent(
			fmt.Sprintf("evt-%05d", i),
			"dimo.status",
			fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64),
			base.Add(time.Duration(i)*time.Second),
		))
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	var file string
	require.NoError(t, filepath.WalkDir(dataPath, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			file = p
			if info, e := d.Info(); e == nil {
				totalBytes += info.Size()
			}
		}
		return nil
	}))
	require.NotEmpty(t, file, "expected a parquet file on disk")

	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT format_version::VARCHAR FROM parquet_file_metadata(%s)", sqlString(file))).Scan(&formatVersion))
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT compression FROM parquet_metadata(%s) WHERE row_group_id = 0 AND path_in_schema = 'subject'", sqlString(file))).Scan(&compression))
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		`SELECT encodings FROM parquet_metadata(%s) WHERE row_group_id = 0 AND path_in_schema = 'time'`, sqlString(file))).Scan(&timeEncodings))
	return formatVersion, compression, timeEncodings, totalBytes
}

// TestWriter_ParquetVersion proves the parquet_version option reaches DuckLake's
// writer and is worth defaulting to 2: v2 bumps the file format version, switches
// the sorted "time" column to delta packing, and shrinks the layer regardless of
// the (snappy default) codec.
func TestWriter_ParquetVersion(t *testing.T) {
	t.Parallel()
	v1fmt, v1comp, v1enc, v1bytes := writeAndInspect(t, "1")
	v2fmt, v2comp, v2enc, v2bytes := writeAndInspect(t, "2")

	// The option reaches the writer: v2 bumps the file format version and
	// delta-packs the sorted "time" column; v1 leaves it plain.
	assert.Equal(t, "1", v1fmt)
	assert.Equal(t, "2", v2fmt)
	assert.NotContains(t, v1enc, "DELTA", "v1 must not delta-pack the time column")
	assert.Contains(t, v2enc, "DELTA", "v2 must delta-pack the sorted time column")

	// Compression is independent of parquet_version — both stay the default snappy.
	assert.Equal(t, "SNAPPY", v1comp)
	assert.Equal(t, "SNAPPY", v2comp)

	// The payoff: v2 shrinks the sorted time-series layer on top of the codec.
	t.Logf("parquet bytes: v1=%d v2=%d (%.1f%%)", v1bytes, v2bytes, 100*float64(v2bytes-v1bytes)/float64(v1bytes))
	assert.Less(t, v2bytes, v1bytes, "parquet v2 must shrink the sorted raw layer vs v1")
}

// TestCompressionLiteral pins the codec allow-list: known codecs pass (case-
// folded), and empty/unknown values fall back to snappy rather than reaching
// set_option and wedging boot.
func TestCompressionLiteral(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		"":             "snappy",
		"snappy":       "snappy",
		"zstd":         "zstd",
		"ZSTD":         "zstd",
		"lz4":          "lz4",
		"uncompressed": "uncompressed",
		"snapy":        "snappy", // typo → default, not a boot wedge
		"gzip":         "snappy", // not in the documented allow-list → default
	} {
		assert.Equal(t, want, compressionLiteral(in), "compressionLiteral(%q)", in)
	}
}

// TestWriter_CompressionOverride proves Config.Compression reaches the writer:
// the snappy default is overridable back to zstd (storage-constrained deploys),
// so the throughput/size tradeoff is not a one-way door.
func TestWriter_CompressionOverride(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	l, err := Open(ctx, Config{
		CatalogDSN:  filepath.Join(dir, "meta.ducklake"),
		DataPath:    dataPath,
		Compression: "zstd",
	})
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	defer w.Close() //nolint:errcheck

	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range 2000 {
		bundle = append(bundle, testEvent(fmt.Sprintf("evt-%05d", i), "dimo.status",
			fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64), base.Add(time.Duration(i)*time.Second)))
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))

	var file string
	require.NoError(t, filepath.WalkDir(dataPath, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			file = p
		}
		return nil
	}))
	require.NotEmpty(t, file)
	var compression string
	require.NoError(t, l.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT compression FROM parquet_metadata(%s) WHERE row_group_id = 0 AND path_in_schema = 'subject'", sqlString(file))).
		Scan(&compression))
	assert.Equal(t, "ZSTD", compression, "Config.Compression must override the snappy default")
}
