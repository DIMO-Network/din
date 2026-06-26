// Package blobcrypt seals externalized cloudevent blob payloads with
// AES-256-GCM. The Parquet lake gets at-rest protection from DuckLake ENCRYPTED
// (key in the Postgres catalog), but the >1MB payloads externalized to the blob
// bucket are not in the lake, so without this they'd have only S3 SSE — readable
// by anything holding an S3 GET credential. blobcrypt closes that: the key lives
// in the pod secret (BLOB_ENCRYPTION_KEY), never in the bucket, so bucket access
// alone yields ciphertext. din seals on write; dq opens on read with the same key.
//
// Wire format: magic("DBE1") || nonce(12) || ciphertext+tag. The magic lets a
// reader tell a sealed blob from a legacy plaintext one and stay compatible
// during rollout. dq carries a byte-identical copy of this format.
package blobcrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// magic prefixes every sealed blob. Changing it breaks read compatibility with
// already-written blobs and with dq — treat as a wire constant.
var magic = []byte("DBE1")

// Cipher seals and opens blob payloads with one AES-256 key.
type Cipher struct{ aead cipher.AEAD }

// NewCipher builds a Cipher from a base64-encoded 32-byte key. An empty key
// returns (nil, nil) so callers treat "no key" as "no encryption" without a
// special case; a malformed key is an error (fail fast at startup).
func NewCipher(b64Key string) (*Cipher, error) {
	if b64Key == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: BLOB_ENCRYPTION_KEY is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("blobcrypt: BLOB_ENCRYPTION_KEY must decode to 32 bytes (AES-256), got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// Seal returns magic || nonce || ciphertext+tag. aad (the object key) is
// authenticated but not encrypted, binding the ciphertext to its path so a blob
// can't be swapped to another key's location and still decrypt.
func (c *Cipher) Seal(aad string, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("blobcrypt: nonce: %w", err)
	}
	out := make([]byte, 0, len(magic)+len(nonce)+len(plaintext)+c.aead.Overhead())
	out = append(out, magic...)
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, []byte(aad)), nil
}

// Open decrypts a sealed blob. A blob without the magic prefix is returned
// unchanged (legacy/plaintext), so a reader with a key set still reads blobs
// written before the key existed. aad must match what Seal used (the object key).
func (c *Cipher) Open(aad string, blob []byte) ([]byte, error) {
	if !IsSealed(blob) {
		return blob, nil
	}
	rest := blob[len(magic):]
	ns := c.aead.NonceSize()
	if len(rest) < ns {
		return nil, errors.New("blobcrypt: sealed blob too short")
	}
	nonce, ct := rest[:ns], rest[ns:]
	pt, err := c.aead.Open(nil, nonce, ct, []byte(aad))
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: open %q: %w", aad, err)
	}
	return pt, nil
}

// IsSealed reports whether b carries the blobcrypt magic prefix.
func IsSealed(b []byte) bool {
	return len(b) >= len(magic) && bytes.Equal(b[:len(magic)], magic)
}
