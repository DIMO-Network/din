package blobcrypt

import (
	"context"

	"github.com/DIMO-Network/din/internal/objstore"
)

// encStore wraps an objstore.Store and seals every payload before it's written,
// using the object key as AEAD additional data. Reads/lists pass through.
type encStore struct {
	objstore.Store
	c *Cipher
}

// WrapStore returns a Store that seals payloads on PutObject. A nil Cipher (no
// BLOB_ENCRYPTION_KEY) returns the underlying store unchanged, so blob encryption
// is opt-in and the write path is identical when it's off.
func WrapStore(s objstore.Store, c *Cipher) objstore.Store {
	if c == nil {
		return s
	}
	return &encStore{Store: s, c: c}
}

func (e *encStore) PutObject(ctx context.Context, key string, body []byte) error {
	sealed, err := e.c.Seal(key, body)
	if err != nil {
		return err
	}
	return e.Store.PutObject(ctx, key, sealed)
}
