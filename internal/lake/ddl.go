package lake

// RawTable is the DuckLake table holding raw cloudevents.
const RawTable = "raw_events"

// rawEventsDDL mirrors cloudevent/parquet.ParquetRow column for column so
// that historical DIS bundles (same encoder) register cleanly via
// ducklake_add_data_files, and so dq's readers see one schema across
// backfilled and native files. Partitioning and sort order are catalog
// metadata applied to data DuckLake writes; "time" is quoted throughout
// because it is a DuckDB keyword.
const rawEventsCreate = `CREATE TABLE IF NOT EXISTS lake.raw_events (
		subject VARCHAR,
		"time" TIMESTAMP WITH TIME ZONE,
		type VARCHAR,
		id VARCHAR,
		source VARCHAR,
		producer VARCHAR,
		data_content_type VARCHAR,
		data_version VARCHAR,
		extras VARCHAR,
		data VARCHAR,
		data_base64 BLOB,
		data_index_key VARCHAR,
		voids_id VARCHAR)`

// rawEventsLayout is the partition + sort layout, kept separate from the CREATE
// so it can be re-asserted on every boot. Backfill RESETs partitioning for its
// registration window and restores it in a defer; a SIGKILL skips the defer, so
// re-asserting at startup is what stops a crash from leaving raw_events
// permanently unpartitioned (CHD-23). The ALTERs are idempotent.
var rawEventsLayout = []string{
	`ALTER TABLE lake.raw_events SET PARTITIONED BY (type, day("time"))`,
	`ALTER TABLE lake.raw_events SET SORTED BY (subject, "time")`,
}

// rawEventsDDL is the full first-boot DDL: create then apply the layout.
var rawEventsDDL = append([]string{rawEventsCreate}, rawEventsLayout...)
