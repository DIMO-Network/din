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

// writeEncBundle opens a lake (encrypted or not), writes one single-day,
// single-type bundle (one parquet file), and returns that file's path plus the
// still-open lake so the caller can query it. The lake closes on test cleanup.
func writeEncBundle(t *testing.T, encrypted bool) (file string, l *Lake) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "data")
	l, err := Open(ctx, Config{
		CatalogDSN: filepath.Join(dir, "meta.ducklake"),
		DataPath:   dataPath,
		Encrypted:  encrypted,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	w, err := l.NewWriter(ctx, RawTable)
	require.NoError(t, err)
	base := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	var bundle []cloudevent.StoredEvent
	for i := range 3000 {
		bundle = append(bundle, testEvent(
			fmt.Sprintf("evt-%05d", i), "dimo.status",
			fmt.Sprintf("did:erc721:137:0xVeh:%d", i%64),
			base.Add(time.Duration(i)*time.Second)))
	}
	require.NoError(t, w.WriteBundle(ctx, bundle))
	require.NoError(t, w.Close())

	require.NoError(t, filepath.WalkDir(dataPath, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			file = p
		}
		return nil
	}))
	require.NotEmpty(t, file, "expected a parquet file on disk")
	return file, l
}

// TestWriter_Encrypted proves Config.Encrypted reaches DuckLake's ATTACH and
// gives real at-rest protection: reads through the catalog stay transparent,
// but the raw data file on disk is unreadable as plain parquet (the per-file
// key lives only in the catalog). The non-encrypted control rules out a false
// positive where read_parquet would have failed for an unrelated reason.
func TestWriter_Encrypted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	encFile, encLake := writeEncBundle(t, true)

	// Transparent read through the catalog: DuckLake fetches the key and decrypts.
	var n int
	require.NoError(t, encLake.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM lake.raw_events").Scan(&n))
	assert.Equal(t, 3000, n, "encrypted lake must read back through the catalog")

	// The catalog actually persisted a per-file key — guards against a future
	// DuckLake change making ENCRYPTED a silent no-op while the raw read still
	// happens to fail for some other reason.
	var keyed int
	require.NoError(t, encLake.DB().QueryRowContext(ctx,
		"SELECT count(*) FROM __ducklake_metadata_lake.ducklake_data_file WHERE encryption_key IS NOT NULL AND encryption_key != ''").Scan(&keyed))
	assert.Positive(t, keyed, "catalog must store a per-file encryption key")

	// At-rest proof: reading the file directly (no catalog, no key) must fail,
	// and fail *because it is encrypted*, not for some unrelated reason.
	var dummy int
	err := encLake.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM read_parquet(%s)", sqlString(encFile))).Scan(&dummy)
	require.Error(t, err, "an encrypted data file must not be readable as plain parquet")
	assert.Contains(t, strings.ToLower(err.Error()), "encrypt",
		"raw read must fail specifically because the file is encrypted")

	// Control: a non-encrypted lake's file IS directly readable, so the error
	// above is the encryption, not some unrelated read failure.
	plainFile, plainLake := writeEncBundle(t, false)
	require.NoError(t, plainLake.DB().QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(*) FROM read_parquet(%s)", sqlString(plainFile))).Scan(&dummy),
		"a non-encrypted data file must be directly readable")
	assert.Equal(t, 3000, dummy)
}
