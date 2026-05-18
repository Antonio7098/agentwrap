package testkit

import (
	"os"
	"strings"
	"testing"
)

func TestLoadJSONLFixtures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        string
		wantRecords int
		wantErrors  int
		wantUnknown bool
	}{
		{name: "normal", path: "testdata/events/normal.jsonl", wantRecords: 3},
		{name: "unknown", path: "testdata/events/unknown.jsonl", wantRecords: 3, wantUnknown: true},
		{name: "malformed", path: "testdata/events/malformed.jsonl", wantRecords: 3, wantErrors: 1},
		{name: "partial", path: "testdata/events/partial.jsonl", wantRecords: 2, wantErrors: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.Open(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			records, err := LoadJSONL(file)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != tc.wantRecords {
				t.Fatalf("len(records) = %d, want %d", len(records), tc.wantRecords)
			}

			var gotErrors int
			var gotUnknown bool
			for _, record := range records {
				if record.Raw == "" {
					t.Fatalf("record %d did not preserve raw input", record.Position)
				}
				if record.Err != nil {
					gotErrors++
				}
				if record.Type == "runtime.future_event" {
					gotUnknown = true
				}
			}
			if gotErrors != tc.wantErrors {
				t.Fatalf("decode errors = %d, want %d", gotErrors, tc.wantErrors)
			}
			if gotUnknown != tc.wantUnknown {
				t.Fatalf("unknown event presence = %v, want %v", gotUnknown, tc.wantUnknown)
			}
		})
	}
}

func TestLoadJSONLPreservesRawMalformedRecord(t *testing.T) {
	records, err := LoadJSONL(strings.NewReader("{bad json\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if records[0].Raw != "{bad json" {
		t.Fatalf("Raw = %q", records[0].Raw)
	}
	if records[0].Err == nil {
		t.Fatal("Err is nil, want decode error")
	}
}
