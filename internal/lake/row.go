package lake

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
)

// emptyExtrasJSON is the extras column value for an event with no non-column
// header fields — the common case. A package const avoids per-row allocation.
const emptyExtrasJSON = "{}"

// rawEventColumnCount is the raw_events column arity (matches ddl.go's CREATE
// and fillRowArgs's DDL order). The appender reuses one slice of this length.
const rawEventColumnCount = 13

// rowArgs maps a StoredEvent onto raw_events columns with the same
// semantics as cloudevent/parquet.convertEvent, keeping native rows
// byte-compatible with backfilled DIS bundles: extras carries the
// non-column header fields as JSON, exactly one of data/data_base64 is
// set (NULL otherwise, like the parquet encoder's optional columns), and
// data_base64 holds the base64 text bytes verbatim. voids_id carries the
// tombstone pointer (NULL when empty), matching ParquetRow so backfilled
// DIS bundles register cleanly and dq can resolve voiding.
//
// "time" is truncated to milliseconds: the legacy DIS parquet encoder wrote
// timestamp(millisecond), so a native row and the same event registered from a
// backfilled bundle must carry an identical value or reader dedup — keyed on
// (subject, "time", ...) — would treat them as distinct and fail to collapse
// the native/backfill overlap (SR review #6).
func rowArgs(event *cloudevent.StoredEvent) ([]driver.Value, error) {
	args := make([]driver.Value, rawEventColumnCount)
	if err := fillRowArgs(args, event); err != nil {
		return nil, err
	}
	return args, nil
}

// fillRowArgs writes event's raw_events column values into dst in DDL order, so
// the appender can reuse one backing slice across a whole bundle (100k+
// rows/bundle) instead of heap-allocating a fresh slice per row. The payload
// columns (data, extras) are passed as []byte to skip a string copy of the
// largest column — DuckDB's appender validates the UTF-8 VARCHAR contract on the
// C side whether given a string or []byte, so poison-row detection is unchanged.
func fillRowArgs(dst []driver.Value, event *cloudevent.StoredEvent) error {
	var extrasJSON driver.Value = emptyExtrasJSON
	if extras := cloudevent.AddNonColumnFieldsToExtras(&event.CloudEventHeader); extras != nil {
		b, err := json.Marshal(extras)
		if err != nil {
			return fmt.Errorf("marshaling extras: %w", err)
		}
		extrasJSON = b // []byte: avoids the string(b) copy in the rare has-extras case
	}

	var data, dataBase64, dataIndexKey driver.Value
	switch {
	case event.DataBase64 != "":
		dataBase64 = []byte(event.DataBase64)
	case len(event.Data) > 0:
		data = []byte(event.Data) // the largest column — pass bytes, not a string copy
	}
	if event.DataIndexKey != "" {
		dataIndexKey = event.DataIndexKey
	}
	var voidsID driver.Value
	if event.VoidsID != "" {
		voidsID = event.VoidsID
	}

	dst[0] = event.Subject
	dst[1] = event.Time.UTC().Truncate(time.Millisecond)
	dst[2] = event.Type
	dst[3] = event.ID
	dst[4] = event.Source
	dst[5] = event.Producer
	dst[6] = event.DataContentType
	dst[7] = event.DataVersion
	dst[8] = extrasJSON
	dst[9] = data
	dst[10] = dataBase64
	dst[11] = dataIndexKey
	dst[12] = voidsID
	return nil
}
