package lake

// RawTable is the DuckLake table holding raw cloudevents.
const RawTable = "raw_events"

// rawEventsDDL mirrors cloudevent/parquet.ParquetRow column for column so
// that historical DIS bundles (same encoder) register cleanly via
// ducklake_add_data_files, and so dq's readers see one schema across
// backfilled and native files. Partitioning and sort order are catalog
// metadata applied to data DuckLake writes; "time" is quoted throughout
// because it is a DuckDB keyword.
var rawEventsDDL = []string{
	`CREATE TABLE IF NOT EXISTS lake.raw_events (
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
		voids_id VARCHAR)`,
	`ALTER TABLE lake.raw_events SET PARTITIONED BY (type, day("time"))`,
	`ALTER TABLE lake.raw_events SET SORTED BY (subject, "time")`,
}
