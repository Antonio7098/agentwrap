package testkit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// EventRecord is private test support for structured fixture loading. It is
// not the public event contract for the SDK.
type EventRecord struct {
	Position int
	Raw      string
	Type     string
	Payload  map[string]any
	Err      error
}

// LoadJSONL reads structured test fixture records while preserving raw lines
// and decode errors for malformed or partial streams.
func LoadJSONL(r io.Reader) ([]EventRecord, error) {
	scanner := bufio.NewScanner(r)
	var records []EventRecord
	for scanner.Scan() {
		raw := scanner.Text()
		record := EventRecord{Position: len(records) + 1, Raw: raw}
		var decoded struct {
			Type    string         `json:"type"`
			Payload map[string]any `json:"payload"`
		}
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			record.Err = fmt.Errorf("decode fixture event %d: %w", record.Position, err)
			records = append(records, record)
			continue
		}
		record.Type = decoded.Type
		record.Payload = decoded.Payload
		if record.Payload == nil {
			record.Payload = map[string]any{}
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return records, err
	}
	return records, nil
}
