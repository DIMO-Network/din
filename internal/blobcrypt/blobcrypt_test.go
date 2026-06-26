package blobcrypt

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKey(t *testing.T) string {
	t.Helper()
	k := make([]byte, 32)
	_, err := rand.Read(k)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(k)
}

func TestCipher_RoundTrip(t *testing.T) {
	c, err := NewCipher(testKey(t))
	require.NoError(t, err)
	require.NotNil(t, c)

	const key = "cloudevent/blobs/sub/2026/06/26/uuid"
	plaintext := []byte("sensitive vehicle payload \x00\x01\x02 with binary")
	sealed, err := c.Seal(key, plaintext)
	require.NoError(t, err)

	assert.True(t, IsSealed(sealed), "sealed blob must carry the magic prefix")
	assert.NotContains(t, string(sealed), "sensitive", "plaintext must not survive in the ciphertext")

	got, err := c.Open(key, sealed)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestCipher_WrongKeyFails(t *testing.T) {
	c1, _ := NewCipher(testKey(t))
	c2, _ := NewCipher(testKey(t))
	sealed, err := c1.Seal("k", []byte("secret"))
	require.NoError(t, err)
	_, err = c2.Open("k", sealed)
	require.Error(t, err, "a different key must not decrypt")
}

func TestCipher_AADBindsToKey(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	sealed, err := c.Seal("key-A", []byte("secret"))
	require.NoError(t, err)
	_, err = c.Open("key-B", sealed)
	require.Error(t, err, "ciphertext bound to one object key must not open under another")
}

func TestCipher_TamperDetected(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	sealed, _ := c.Seal("k", []byte("secret"))
	sealed[len(sealed)-1] ^= 0xff // flip a tag bit
	_, err := c.Open("k", sealed)
	require.Error(t, err)
}

func TestCipher_PlaintextPassthrough(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	legacy := []byte("raw legacy payload written before the key existed")
	got, err := c.Open("k", legacy)
	require.NoError(t, err, "an unsealed (no-magic) blob is returned as-is for rollout compatibility")
	assert.Equal(t, legacy, got)
}

func TestNewCipher_Validation(t *testing.T) {
	c, err := NewCipher("") // empty → nil cipher, no error (encryption off)
	require.NoError(t, err)
	assert.Nil(t, c)

	_, err = NewCipher("not!base64!")
	require.Error(t, err)

	short := base64.StdEncoding.EncodeToString(make([]byte, 16)) // AES-128, not 256
	_, err = NewCipher(short)
	require.Error(t, err)
}

// TestGoldenVector pins the wire format: this exact (key, sealed, aad,
// plaintext) tuple is also pinned in dq's blobcrypt_test. If din's format drifts,
// this Open fails here; if dq's drifts, dq's copy fails — so neither repo can
// change the format without the other noticing.
func TestGoldenVector(t *testing.T) {
	const (
		keyB64    = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio="
		sealedB64 = "REJFMfpbx2YHkkRYAdtRPzuxzz7dwfQJNizKGPt2rTfyuQxLDF656mDN5H8zlpCmikO3wJcKpg=="
		aad       = "cloudevent/blobs/golden"
		want      = "din<->dq blob format v1"
	)
	c, err := NewCipher(keyB64)
	require.NoError(t, err)
	sealed, err := base64.StdEncoding.DecodeString(sealedB64)
	require.NoError(t, err)
	got, err := c.Open(aad, sealed)
	require.NoError(t, err)
	assert.Equal(t, want, string(got))
}

type fakeStore struct{ body []byte }

func (f *fakeStore) PutObject(_ context.Context, _ string, body []byte) error {
	f.body = body
	return nil
}
func (f *fakeStore) ListObjectsV2(_ context.Context, _ string) ([]objstore.ObjectInfo, error) {
	return nil, nil
}

func TestWrapStore_SealsOnPut(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	fs := &fakeStore{}
	require.NoError(t, WrapStore(fs, c).PutObject(context.Background(), "blob/key", []byte("plaintext-secret")))
	assert.True(t, IsSealed(fs.body), "wrapped store must seal before writing")
	got, err := c.Open("blob/key", fs.body)
	require.NoError(t, err)
	assert.Equal(t, []byte("plaintext-secret"), got)
}

func TestWrapStore_NilCipherPassthrough(t *testing.T) {
	fs := &fakeStore{}
	require.NoError(t, WrapStore(fs, nil).PutObject(context.Background(), "k", []byte("plain")))
	assert.False(t, IsSealed(fs.body), "no key → no seal, identical to the unwrapped store")
	assert.Equal(t, []byte("plain"), fs.body)
}
