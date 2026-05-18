package testkit

import (
	"os"
	"reflect"
	"testing"
)

func TestRunFakeLifecycleSuccess(t *testing.T) {
	records := loadFixture(t, "testdata/events/normal.jsonl")

	states, err := RunFakeLifecycle(records)

	if err != nil {
		t.Fatal(err)
	}
	want := []LifecycleState{StateStarting, StateRunning, StateCompleted}
	if !reflect.DeepEqual(states, want) {
		t.Fatalf("states = %#v, want %#v", states, want)
	}
}

func TestRunFakeLifecycleFailureCases(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "malformed", path: "testdata/events/malformed.jsonl"},
		{name: "explicit failure", path: "testdata/events/lifecycle_failure.jsonl"},
		{name: "partial", path: "testdata/events/partial.jsonl"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := loadFixture(t, tc.path)

			states, err := RunFakeLifecycle(records)

			if err == nil {
				t.Fatal("RunFakeLifecycle returned nil error")
			}
			if states[len(states)-1] != StateFailed {
				t.Fatalf("final state = %q, want %q", states[len(states)-1], StateFailed)
			}
		})
	}
}

func loadFixture(t *testing.T, path string) []EventRecord {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	records, err := LoadJSONL(file)
	if err != nil {
		t.Fatal(err)
	}
	return records
}
