package lake

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"github.com/DIMO-Network/cloudevent"
)

// rowArgs maps a StoredEvent onto raw_events columns with the same
// semantics as cloudevent/parquet.convertEvent, keeping native rows
// byte-compatible with backfilled DIS bundles: extras carries the
// non-column header fields as JSON, exactly one of data/data_base64 is
// set (NULL otherwise, like the parquet encoder's optional columns), and
// data_base64 holds the base64 text bytes verbatim. VoidsID is not
// stored, matching ParquetRow.
func rowArgs(event *cloudevent.StoredEvent) ([]driver.Value, error) {
	extrasJSON := []byte("{}")
	if extras := cloudevent.AddNonColumnFieldsToExtras(&event.CloudEventHeader); extras != nil {
		var err error
		if extrasJSON, err = json.Marshal(extras); err != nil {
			return nil, fmt.Errorf("marshaling extras: %w", err)
		}
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
	}, nil
}
