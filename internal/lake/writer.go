package lake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/DIMO-Network/cloudevent"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

// Writer appends raw cloudevents to one lake table. Each WriteBundle is a
// single DuckLake transaction — one snapshot, one or more Parquet files
// split by partition — and returns only once the commit is durable in the
// catalog, so the caller can ack its WAL messages. Safe for concurrent
// use; bundles serialize on one pinned connection.
//
// Writes are a blind append: there is NO at-rest dedup here (no anti-join, no
// ON CONFLICT). Delivery is at-least-once — NATS Nats-Msg-Id collapses retries
// only within the stream's DuplicateWindow, so beyond it (consumer lag, broker
// failover, replay) duplicate rows persist in raw_events. Dedup is delegated to
// readers: dq's INSERT anti-join for decoded signals/events and its read-side
// QUALIFY for queries. Do not assume raw_events rows are unique (SR review #5).
type Writer struct {
	mu    sync.Mutex
	conn  *sql.Conn
	table string
}

// NewWriter pins a dedicated connection for appends to table.
func (l *Lake) NewWriter(ctx context.Context, table string) (*Writer, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("lake writer: %w", err)
	}
	return &Writer{conn: conn, table: table}, nil
}

// WriteBundle durably persists events; on return, acking is safe. On
// error nothing is committed and the caller's messages redeliver.
func (w *Writer) WriteBundle(ctx context.Context, events []cloudevent.StoredEvent) error {
	if len(events) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// Explicit BEGIN/COMMIT rather than database/sql Tx: the appender
	// needs the raw driver connection, which sql.Tx keeps to itself.
	if _, err := w.conn.ExecContext(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("lake write begin: %w", err)
	}
	if err := w.appendAll(ctx, events); err != nil {
		if _, rbErr := w.conn.ExecContext(ctx, "ROLLBACK"); rbErr != nil {
			return fmt.Errorf("%w (rollback also failed: %v)", err, rbErr)
		}
		return err
	}
	if _, err := w.conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("lake write commit: %w", err)
	}
	return nil
}

func (w *Writer) appendAll(ctx context.Context, events []cloudevent.StoredEvent) error {
	return w.conn.Raw(func(driverConn any) error {
		appender, err := duckdb.NewAppender(driverConn.(*duckdb.Conn), "lake", "main", w.table)
		if err != nil {
			return fmt.Errorf("lake appender: %w", err)
		}
		// Reuse one args slice across the whole bundle — AppendRow reads it during
		// the call and never retains it, so refilling it per row avoids ~100k slice
		// allocations on a full bundle.
		args := make([]driver.Value, rawEventColumnCount)
		for i := range events {
			if err := fillRowArgs(args, &events[i]); err != nil {
				_ = appender.CloseWithCancel(ctx)
				return fmt.Errorf("lake row %d: %w", i, err)
			}
			if err := appender.AppendRow(args...); err != nil {
				_ = appender.CloseWithCancel(ctx)
				return fmt.Errorf("lake append row %d: %w", i, err)
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

// Close releases the pinned connection.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.Close()
}
