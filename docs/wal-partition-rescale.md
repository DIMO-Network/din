# WAL partition rescale runbook (NATS_STREAM_PARTITIONS)

Changing `NATS_STREAM_PARTITIONS` re-routes every subject (the partition token
`pNNN` = hash(subject) % N is baked into the publish subject), so a rescale is
a **drain-and-delete migration**, not a config flip. din refuses to boot
against streams from a different layout (`checkStaleStreams`) — this runbook
is the procedure that error message points at.

Decide the target partition count for your growth horizon ONCE and pay this
migration rarely: partitions bound flush parallelism, and idle partitions are
cheap.

## Procedure (grow 1→N or reshape N→M)

1. **Stop publishes**: scale din ingest to 0 (devices buffer/retry — the
   ingest 503 path is designed for this) or divert at the LB. The WAL keeps
   its backlog; nothing is lost.
2. **Drain the old streams to empty**: leave the sink + decodestream
   consumers running until every old `INGEST_RAW*` stream reports zero
   unacked/pending messages:
   `nats stream report` (or `nats consumer info INGEST_RAW parquet-sink`) —
   `num_pending == 0` and `ack floor == last_seq` for every durable.
3. **Then stop the consumers** (scale din fully to 0).
4. **Delete the old streams**: `nats stream rm INGEST_RAW` (and/or each
   `INGEST_RAW_PNNN` outside the new layout). Deleting before the drain is
   the data-loss mistake this runbook exists to prevent.
5. **Set the new `NATS_STREAM_PARTITIONS`** in the values overlay.
6. **Start din**: `EnsureStreams` provisions the new layout; sinks create
   fresh durables per partition.
7. **Verify**: publishes land (`nats stream report` shows traffic across all
   new partitions), sink commits resume, dq decode lag returns to zero.

## Notes

- Per-vehicle ordering across the rescale: a vehicle's events in the old
  layout are fully persisted (step 2) before any new-layout event is
  accepted (step 6), so ordering holds.
- The dedup window (`Nats-Msg-Id`, 2m) does not span streams — device
  retries from the stopped-ingest window may duplicate across the boundary;
  the read-path dedup collapses them.
- `NATS_STREAM_MAX_BYTES` is per partition — re-derive it when N changes
  (broker volume / N, with headroom).
