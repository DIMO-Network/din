package lake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/DIMO-Network/cloudevent"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// ErrPoisonRow marks a deterministic, row-level rejection — the row's own data
// is unpersistable (marshal failure, or DuckDB rejecting bad UTF-8/precision in
// AppendRow): retrying the identical row will always fail. Transient errors
// (BEGIN/COMMIT, appender creation, the flush's S3 upload + catalog commit,
// connection loss) are NOT wrapped, so the sink can leave them un-acked for
// redelivery instead of permanently dropping healthy, never-persisted events.
var ErrPoisonRow = errors.New("poison row")

// Writer appends raw cloudevents to one lake table. Each WriteBundle is a
// single DuckLake transaction — one snapshot, one or more Parquet files
// split by partition — and returns only once the commit is durable in the
// catalog, so the caller can ack its WAL messages. Safe for concurrent use:
// bundles are round-robined across a pool of pinned connections, so several
// bundles' S3 Parquet uploads + commits overlap instead of serializing on one
// connection (the per-partition write-throughput ceiling).
//
// Writes are a blind append: there is NO at-rest dedup here (no anti-join, no
// ON CONFLICT). Delivery is at-least-once — NATS Nats-Msg-Id collapses retries
// only within the stream's DuplicateWindow, so beyond it (consumer lag, broker
// failover, replay) duplicate rows persist in raw_events. Dedup is delegated to
// readers: dq's INSERT anti-join for decoded signals/events and its read-side
// QUALIFY for queries. Do not assume raw_events rows are unique (SR review #5).
// Because writes carry no ordering, distributing bundles across connections is
// safe.
type Writer struct {
	table string
	conns []*writerConn
	next  atomic.Uint64
}

// writerConn is one pinned connection plus the mutex serializing the bundles
// routed to it (a single DuckDB connection is not safe for concurrent BEGIN/
// append/COMMIT).
type writerConn struct {
	mu   sync.Mutex
	conn *sql.Conn
}

// NewWriter pins a single dedicated connection for appends to table.
func (l *Lake) NewWriter(ctx context.Context, table string) (*Writer, error) {
	return l.NewWriterN(ctx, table, 1)
}

// NewWriterN pins n dedicated connections; WriteBundle round-robins across them
// so up to n bundles commit concurrently. The shared DuckDB pool must have room
// for n connections per writer (see MaxConns sizing in cmd/din).
func (l *Lake) NewWriterN(ctx context.Context, table string, n int) (*Writer, error) {
	if n < 1 {
		n = 1
	}
	w := &Writer{table: table, conns: make([]*writerConn, 0, n)}
	for range n {
		conn, err := l.db.Conn(ctx)
		if err != nil {
			_ = w.Close() // release any already-pinned connections
			return nil, fmt.Errorf("lake writer: %w", err)
		}
		w.conns = append(w.conns, &writerConn{conn: conn})
	}
	return w, nil
}

// WriteBundle durably persists events; on return, acking is safe. On
// error nothing is committed and the caller's messages redeliver.
//
// The BEGIN/append/COMMIT is wrapped in a commit-conflict retry (load review
// #6): under DuckLake optimistic concurrency a lost commit race surfaces as a
// transient "TransactionContext Error", and without a cheap retry here that
// bubbles up to the sink's per-event isolate() bisection (up to ~8 extra commit
// attempts under the writer mutex) and, if it persists, multi-minute AckWait
// redelivery. Only the transient conflict class retries — a deterministic
// ErrPoisonRow (unpersistable row) or any other error fails fast to the sink,
// which handles it (isolate/terminate) exactly as before.
func (w *Writer) WriteBundle(ctx context.Context, events []cloudevent.StoredEvent) error {
	if len(events) == 0 {
		return nil
	}
	// Round-robin to the next connection; concurrent callers land on different
	// connections and proceed in parallel, queueing only when they collide. The
	// modulo stays in the unsigned domain so the index can't go negative once the
	// counter passes the int64 boundary.
	wc := w.conns[int(w.next.Add(1)%uint64(len(w.conns)))]
	wc.mu.Lock()
	defer wc.mu.Unlock()
	conn := wc.conn

	return retryCatalogIf(ctx, func() error {
		return writeBundleOnce(ctx, conn, w.table, events)
	}, isCommitConflict)
}

// writeBundleOnce runs one BEGIN/append/COMMIT attempt. Each attempt leaves the
// pinned connection with no open transaction (it ROLLBACKs on any failure), so a
// commit-conflict retry can start a fresh BEGIN cleanly.
func writeBundleOnce(ctx context.Context, conn *sql.Conn, table string, events []cloudevent.StoredEvent) error {
	// Explicit BEGIN/COMMIT rather than database/sql Tx: the appender
	// needs the raw driver connection, which sql.Tx keeps to itself.
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("lake write begin: %w", err)
	}
	if err := appendAll(ctx, conn, table, events); err != nil {
		// Roll back with an uncancellable ctx: duckdb-go short-circuits ExecContext
		// when ctx is already done, so a ctx-scoped ROLLBACK on a cancelled request
		// would no-op and leave the transaction open on this pinned, reused
		// connection — every future BEGIN then fails ("transaction already active"),
		// wedging the conn for the process lifetime.
		if _, rbErr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); rbErr != nil {
			return fmt.Errorf("%w (rollback also failed: %w)", err, rbErr)
		}
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		// A failed COMMIT can leave the transaction open; reset it with an
		// uncancellable ctx so the pinned conn stays reusable. Ignore the rollback
		// error — DuckDB may have already auto-aborted (ROLLBACK then no-ops).
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return fmt.Errorf("lake write commit: %w", err)
	}
	return nil
}

// isCommitConflict reports whether err is the transient DuckLake optimistic-
// concurrency commit-conflict class — a lost metadata-commit race that a retry
// clears. It is deliberately narrow: a deterministic ErrPoisonRow (or any other
// error) must NOT retry, so the sink's isolate/terminate path still sees it.
// DuckDB surfaces the conflict as a "TransactionContext Error" (the phrasing the
// maintenance retry already keys on); "conflict" is matched too as a defensive
// widening for wording drift across DuckLake versions.
func isCommitConflict(err error) bool {
	if err == nil || errors.Is(err, ErrPoisonRow) {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "transactioncontext error") || strings.Contains(msg, "conflict")
}

func appendAll(ctx context.Context, conn *sql.Conn, table string, events []cloudevent.StoredEvent) error {
	return conn.Raw(func(driverConn any) error {
		// Comma-ok rather than a bare assertion: a failed assertion here panics on the
		// pinned writer conn. Unreachable with the current driver (duckdb-go's only
		// driver.Conn is *duckdb.Conn), but a driver swap should surface an error, not
		// crash the ingest writer.
		dc, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("lake appender: unexpected driver conn type %T", driverConn)
		}
		appender, err := duckdb.NewAppender(dc, "lake", "main", table)
		if err != nil {
			return fmt.Errorf("lake appender: %w", err)
		}
		// Reuse one args slice across the whole bundle — AppendRow reads it during
		// the call and never retains it, so refilling it per row avoids ~100k slice
		// allocations on a full bundle.
		args := make([]driver.Value, rawEventColumnCount)
		for i := range events {
			// fillRowArgs (marshal) and AppendRow (DuckDB rejecting bad
			// UTF-8/precision) are deterministic per-row rejections: tag them
			// ErrPoisonRow so the sink isolates/terminates the row instead of
			// treating it like a transient outage. The flush below (Close) is the
			// only I/O here and is deliberately left untagged (transient).
			if err := fillRowArgs(args, &events[i]); err != nil {
				_ = appender.CloseWithCancel(ctx)
				return fmt.Errorf("lake row %d: %w: %w", i, ErrPoisonRow, err)
			}
			if err := appender.AppendRow(args...); err != nil {
				_ = appender.CloseWithCancel(ctx)
				return fmt.Errorf("lake append row %d: %w: %w", i, ErrPoisonRow, err)
			}
		}
		// CloseWithCancel threads the caller's ctx through the final flush — the
		// real S3 Parquet upload + catalog commit. Plain Close() flushes under
		// context.Background(), so a shutdown (worker ctx canceled past
		// DrainTimeout) could not interrupt a wedged write; this honors that bound.
		if err := appender.CloseWithCancel(ctx); err != nil {
			return fmt.Errorf("lake appender close: %w", err)
		}
		return nil
	})
}

// Close releases all pinned connections.
func (w *Writer) Close() error {
	var firstErr error
	for _, wc := range w.conns {
		wc.mu.Lock()
		if err := wc.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		wc.mu.Unlock()
	}
	return firstErr
}
