// Package fsstore is the local-filesystem objstore.Store implementation for
// single-node deployments. Semantics mirror s3client where consumers can
// tell the difference: keys are slash-separated, List returns
// lexicographically sorted keys, and a missing prefix lists empty.
//
// Durability matches the sink's ack-after-durable contract: PutObject writes
// a temp file in the target directory, fsyncs, then renames, so readers
// globbing the same tree (single-node dq) never observe a partial object and
// an acked write survives a crash.
package fsstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/DIMO-Network/din/internal/objstore"
	"github.com/DIMO-Network/din/internal/split"
)

// Verify Client serves the shared store surface and the narrow consumer
// interfaces, like s3client.Client.
var (
	_ objstore.Store    = (*Client)(nil)
	_ split.ObjectStore = (*Client)(nil)
)

// tempPrefix marks in-flight writes; List hides any dot-named entry.
const tempPrefix = ".tmp-"

// Client stores objects under a root directory.
type Client struct {
	root string
}

// New returns a Client rooted at the absolute directory root, creating it if
// needed.
func New(root string) (*Client, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("fsstore root must be an absolute path, got %q", root)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("creating fsstore root: %w", err)
	}
	return &Client{root: root}, nil
}

func (c *Client) path(key string) string {
	return filepath.Join(c.root, filepath.FromSlash(key))
}

// PutObject durably writes body at key: temp file in the target directory,
// fsync, atomic rename. Replacing an existing key (e.g. a split blob) is atomic
// too.
func (c *Client) PutObject(_ context.Context, key string, body []byte) error {
	target := c.path(key)
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating directories for %s: %w", key, err)
	}
	tmp, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return fmt.Errorf("creating temp file for %s: %w", key, err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(body); err != nil {
		cleanup()
		return fmt.Errorf("writing %s: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("syncing %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("closing %s: %w", key, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("setting mode for %s: %w", key, err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("publishing %s: %w", key, err)
	}
	// fsync the directory: the rename itself is not durable until the
	// directory entry is, and PutObject returning is the sink's ack gate.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ListObjectsV2 lists all objects whose key starts with prefix, sorted
// lexicographically like S3. prefix is a key prefix, not a directory:
// "raw/type=" matches every type partition. A missing prefix lists empty.
func (c *Client) ListObjectsV2(_ context.Context, prefix string) ([]objstore.ObjectInfo, error) {
	// Walk from the deepest directory the prefix fully names so a scan of
	// "raw/type=dimo.status/" doesn't traverse unrelated partitions.
	dirPart, _ := path.Split(prefix)
	walkRoot := filepath.Join(c.root, filepath.FromSlash(dirPart))

	var out []objstore.ObjectInfo
	err := filepath.WalkDir(walkRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil // temp files and editor droppings are not objects
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(c.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // deleted mid-walk
			}
			return err
		}
		out = append(out, objstore.ObjectInfo{Key: key, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", prefix, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
