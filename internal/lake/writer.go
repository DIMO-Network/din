package lake

import (
	"context"
	"database/sql"
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
	if err := w.appendAll(events); err != nil {
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

func (w *Writer) appendAll(events []cloudevent.StoredEvent) error {
	return w.conn.Raw(func(driverConn any) error {
		appender, err := duckdb.NewAppender(driverConn.(*duckdb.Conn), "lake", "main", w.table)
		if err != nil {
			return fmt.Errorf("lake appender: %w", err)
		}
		for i := range events {
			args, err := rowArgs(&events[i])
			if err != nil {
				_ = appender.Close()
				return fmt.Errorf("lake row %d: %w", i, err)
			}
			if err := appender.AppendRow(args...); err != nil {
				_ = appender.Close()
				return fmt.Errorf("lake append row %d: %w", i, err)
			}
		}
		if err := appender.Close(); err != nil {
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
