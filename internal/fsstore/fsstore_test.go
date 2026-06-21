package fsstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T) *Client {
	t.Helper()
	c, err := New(t.TempDir())
	require.NoError(t, err)
	return c
}

func TestNew_RejectsRelativeRoot(t *testing.T) {
	t.Parallel()
	_, err := New("relative/path")
	require.Error(t, err)
}

func TestPutGetRoundTrip(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()

	key := "raw/type=dimo.status/date=2026-06-10/a.parquet"
	require.NoError(t, c.PutObject(ctx, key, []byte("body")))
	got, err := os.ReadFile(c.path(key))
	require.NoError(t, err)
	assert.Equal(t, []byte("body"), got)
}

func TestPutOverwritesAtomically(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()

	key := "decoded/v1/_state/watermark.json"
	require.NoError(t, c.PutObject(ctx, key, []byte(`{"a":"1"}`)))
	require.NoError(t, c.PutObject(ctx, key, []byte(`{"a":"2"}`)))
	got, err := os.ReadFile(c.path(key))
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":"2"}`, string(got))
}

func TestPutLeavesNoTempResidue(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	require.NoError(t, c.PutObject(context.Background(), "raw/a.parquet", []byte("x")))

	entries, err := os.ReadDir(filepath.Join(c.root, "raw"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "a.parquet", entries[0].Name())
}

func TestListObjectsV2_KeyPrefixAcrossDirs(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()
	require.NoError(t, c.PutObject(ctx, "raw/type=dimo.status/date=2026-06-10/b.parquet", []byte("2")))
	require.NoError(t, c.PutObject(ctx, "raw/type=dimo.status/date=2026-06-09/a.parquet", []byte("1")))
	require.NoError(t, c.PutObject(ctx, "raw/type=dimo.fingerprint/date=2026-06-10/c.parquet", []byte("3")))
	require.NoError(t, c.PutObject(ctx, "decoded/v1/x.parquet", []byte("4")))

	// Key prefix, not a directory: matches every type partition.
	all, err := c.ListObjectsV2(ctx, "raw/type=")
	require.NoError(t, err)
	require.Len(t, all, 3)
	for i := 1; i < len(all); i++ {
		assert.Less(t, all[i-1].Key, all[i].Key, "sorted like S3")
	}

	one, err := c.ListObjectsV2(ctx, "raw/type=dimo.status/")
	require.NoError(t, err)
	require.Len(t, one, 2)
	assert.Equal(t, "raw/type=dimo.status/date=2026-06-09/a.parquet", one[0].Key)
	assert.Equal(t, int64(1), one[0].Size)
}

func TestListObjectsV2_MissingPrefixEmpty(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	out, err := c.ListObjectsV2(context.Background(), "raw/type=")
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestListObjectsV2_HidesTempFiles(t *testing.T) {
	t.Parallel()
	c := newClient(t)
	ctx := context.Background()
	require.NoError(t, c.PutObject(ctx, "raw/a.parquet", []byte("x")))
	// Simulate an in-flight write and an editor dropping.
	require.NoError(t, os.WriteFile(filepath.Join(c.root, "raw", tempPrefix+"123"), []byte("partial"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(c.root, "raw", ".DS_Store"), []byte("junk"), 0o644))

	out, err := c.ListObjectsV2(ctx, "raw/")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "raw/a.parquet", out[0].Key)
}
