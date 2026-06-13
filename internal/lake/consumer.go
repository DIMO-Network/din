package lake

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ConsumerProgressTable is the catalog-database table where downstream
// consumers (the dq materializer) report how far they've consumed, so
// expiry never drops a snapshot a live consumer still needs. It is a
// plain table in the `meta` database — deliberately NOT a DuckLake table,
// which would mint a snapshot per cursor update.
const ConsumerProgressTable = "meta.din_consumer_progress"

// consumerProgressDDL creates the progress table. Portable across a
// DuckDB-file `meta` (dev/test) and a Postgres `meta` reached through
// DuckDB's postgres extension (prod): no constraints, since the extension
// doesn't pass all of them through and the upsert is done in app code.
const consumerProgressDDL = `CREATE TABLE IF NOT EXISTS meta.din_consumer_progress (
	consumer VARCHAR,
	snapshot_id BIGINT,
	updated_at TIMESTAMP WITH TIME ZONE)`

// RecordConsumerProgress upserts a consumer's cursor: the highest snapshot
// it has fully processed, stamped now(). This is the exact write a
// downstream consumer makes after committing a materialization batch — dq
// runs the equivalent against the shared catalog DB itself; din exposes it
// here for tests and as the reference for the contract. Upsert is
// delete-then-insert in one transaction (the postgres extension doesn't
// reliably pass ON CONFLICT through).
func (l *Lake) RecordConsumerProgress(ctx context.Context, consumer string, snapshotID int64) error {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	rollback := func() { _, _ = conn.ExecContext(ctx, "ROLLBACK") }
	if _, err := conn.ExecContext(ctx,
		"DELETE FROM meta.din_consumer_progress WHERE consumer = ?", consumer); err != nil {
		rollback()
		return fmt.Errorf("consumer progress delete: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO meta.din_consumer_progress VALUES (?, ?, now())", consumer, snapshotID); err != nil {
		rollback()
		return fmt.Errorf("consumer progress insert: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("consumer progress commit: %w", err)
	}
	return nil
}

// ConsumerFloor is the lowest cursor among consumers that have reported
// within staleness — the snapshot id below which expiry is free to run.
// ok is false when no live consumer exists (expiry then falls back to
// pure time-based retention). A consumer quiet for longer than staleness
// is presumed dead and excluded, so a crashed consumer can't wedge the
// lake; the tradeoff is that it must rescan if it returns past retention.
func (l *Lake) ConsumerFloor(ctx context.Context, staleness time.Duration) (floor int64, ok bool, err error) {
	var v sql.NullInt64
	err = l.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT MIN(snapshot_id) FROM meta.din_consumer_progress
		 WHERE updated_at > now() - INTERVAL '%d seconds'`, int64(staleness.Seconds())),
	).Scan(&v)
	if err != nil {
		return 0, false, fmt.Errorf("consumer floor: %w", err)
	}
	return v.Int64, v.Valid, nil
}
