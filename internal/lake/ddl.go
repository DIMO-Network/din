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
		voids_id VARCHAR)
	PARTITIONED BY ` + rawEventsPartitionSpec + `
	SORTED BY ` + rawEventsSortSpec

// rawEventsLayout is the partition + sort layout as repair ALTERs.
//
// The time partition is the year/month/day TRIPLE: DuckLake's year()/month()/
// day() are component extractions (day(x) alone is day-of-month 1-31, NOT a
// date — the original single-day() spec cycled 31 buckets mixing every month),
// evaluated in the session TimeZone (UTC, pinned per-conn in lake.go). Known
// trade-off of daily grain: ducklake_merge_adjacent_files only consolidates
// within one partition, so a low-volume type (a few events/day) keeps one tiny
// per-day file forever; accepted for layout legibility.
//
// The ALTER form is kept for repair: reassertLayout re-applies it when a crashed
// backfill left it RESET. Backfill RESETs partitioning for its registration window
// and restores it in a defer; a SIGKILL skips the defer, so re-asserting is what
// stops a crash from leaving raw_events permanently unpartitioned (CHD-23).
//
// These ALTERs are NOT idempotent: DuckLake bumps schema_version on every SET even
// when the spec is unchanged (still true of SET SORTED BY on DuckDB 2.0), which
// churns the catalog (and renames inline-data tables — the dq materializer crash).
// So they must NOT run blindly on every boot: first creation declares the layout
// in rawEventsCreate, and reassertLayout re-applies an ALTER only when
// isPartitioned/isSorted report that piece of the layout is currently missing.
var rawEventsLayout = []string{
	`ALTER TABLE lake.raw_events SET PARTITIONED BY ` + rawEventsPartitionSpec,
	`ALTER TABLE lake.raw_events SET SORTED BY ` + rawEventsSortSpec,
}

// rawEventsPartitionSpec and rawEventsSortSpec are shared by the CREATE and the
// repair ALTERs so the two can never describe different layouts.
const (
	rawEventsPartitionSpec = `(type, year("time"), month("time"), day("time"))`
	rawEventsSortSpec      = `(subject, "time")`
)

// rawEventsDDL is the full first-boot DDL. The layout is declared in the CREATE
// (DuckLake 1.1 DDL), so the table and its layout commit as one statement: the
// old CREATE-then-two-ALTERs sequence auto-committed each statement on its own,
// and a failure between them left the table partitioned but unsorted (or bare)
// until reassertLayout noticed.
var rawEventsDDL = []string{rawEventsCreate}
