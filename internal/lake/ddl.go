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

// rawEventsLayout is the partition + sort layout.
//
// The time partition is the year/month/day TRIPLE: DuckLake's year()/month()/
// day() are component extractions (day(x) alone is day-of-month 1-31, NOT a
// date — the original single-day() spec cycled 31 buckets mixing every month),
// evaluated in the session TimeZone (UTC, pinned per-conn in lake.go). Known
// trade-off of daily grain: ducklake_merge_adjacent_files only consolidates
// within one partition, so a low-volume type (a few events/day) keeps one tiny
// per-day file forever; accepted for layout legibility.
//
// It is kept separate from the CREATE so it can be re-asserted when a crashed
// backfill left it RESET. Backfill RESETs
// partitioning for its registration window and restores it in a defer; a SIGKILL
// skips the defer, so re-asserting is what stops a crash from leaving raw_events
// permanently unpartitioned (CHD-23).
//
// These ALTERs are NOT idempotent: DuckLake bumps schema_version on every SET even
// when the spec is unchanged, which churns the catalog (and renames inline-data
// tables — the dq materializer crash). So they must NOT run blindly on every boot:
// tryEnsureSchema applies them only on first creation, and reassertLayout re-applies
// them only when isPartitioned reports the layout is currently missing.
var rawEventsLayout = []string{
	`ALTER TABLE lake.raw_events SET PARTITIONED BY (type, year("time"), month("time"), day("time"))`,
	`ALTER TABLE lake.raw_events SET SORTED BY (subject, "time")`,
}

// rawEventsDDL is the full first-boot DDL: create then apply the layout.
var rawEventsDDL = append([]string{rawEventsCreate}, rawEventsLayout...)
