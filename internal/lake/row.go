package lake

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
)

// emptyExtrasJSON is the extras column value for an event with no non-column
// header fields — the common case. A package const avoids per-row allocation.
const emptyExtrasJSON = "{}"

// rowArgs maps a StoredEvent onto raw_events columns with the same
// semantics as cloudevent/parquet.convertEvent, keeping native rows
// byte-compatible with backfilled DIS bundles: extras carries the
// non-column header fields as JSON, exactly one of data/data_base64 is
// set (NULL otherwise, like the parquet encoder's optional columns), and
// data_base64 holds the base64 text bytes verbatim. voids_id carries the
// tombstone pointer (NULL when empty), matching ParquetRow so backfilled
// DIS bundles register cleanly and dq can resolve voiding.
func rowArgs(event *cloudevent.StoredEvent) ([]driver.Value, error) {
	extrasJSON := emptyExtrasJSON
	if extras := cloudevent.AddNonColumnFieldsToExtras(&event.CloudEventHeader); extras != nil {
		b, err := json.Marshal(extras)
		if err != nil {
			return nil, fmt.Errorf("marshaling extras: %w", err)
		}
		extrasJSON = string(b)
	}

	var data, dataBase64, dataIndexKey driver.Value
	switch {
	case event.DataBase64 != "":
		dataBase64 = []byte(event.DataBase64)
	case len(event.Data) > 0:
		data = string(event.Data)
	}
	if event.DataIndexKey != "" {
		dataIndexKey = event.DataIndexKey
	}
	var voidsID driver.Value
	if event.VoidsID != "" {
		voidsID = event.VoidsID
	}

	return []driver.Value{
		event.Subject,
		event.Time.UTC(),
		event.Type,
		event.ID,
		event.Source,
		event.Producer,
		event.DataContentType,
		event.DataVersion,
		string(extrasJSON),
		data,
		dataBase64,
		dataIndexKey,
		voidsID,
	}, nil
}
