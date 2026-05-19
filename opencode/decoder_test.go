package opencode

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/antonioborgerees/agentwrap"
)

func TestDecodeNativeLine(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		pos       int64
		wantType  string
		wantSID   string
		wantRaw   string
		wantError bool
	}{
		{
			name:     "valid typed event",
			line:     `{"type":"text","sessionID":"ses_1"}`,
			pos:      3,
			wantType: "text",
			wantSID:  "ses_1",
			wantRaw:  `{"type":"text","sessionID":"ses_1"}`,
		},
		{
			name:      "malformed json",
			line:      `{"type":`,
			pos:       9,
			wantError: true,
		},
		{
			name:      "missing type",
			line:      `{"sessionID":"ses_1"}`,
			pos:       4,
			wantError: true,
		},
		{
			name:      "non string type",
			line:      `{"type":123}`,
			pos:       5,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := decodeNativeLine([]byte(tt.line), tt.pos)
			if tt.wantError {
				var d *decodeError
				if !errors.As(err, &d) {
					t.Fatalf("expected decodeError, got %T %v", err, err)
				}
				if d.line != tt.pos {
					t.Fatalf("line = %d, want %d", d.line, tt.pos)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if record.Type != tt.wantType || record.SessionID != tt.wantSID || record.Line != tt.pos {
				t.Fatalf("unexpected record: %#v", record)
			}
			if string(record.Raw) != tt.wantRaw {
				t.Fatalf("raw not preserved: %q", string(record.Raw))
			}
		})
	}
}

func TestProjectNativeEvents(t *testing.T) {
	tests := []struct {
		name         string
		line         string
		wantCategory agentwrap.EventCategory
		wantFinal    bool
	}{
		{name: "progress", line: `{"type":"step_start","sessionID":"ses_1"}`, wantCategory: agentwrap.EventProgress},
		{name: "message", line: `{"type":"text","timestamp":1710000000000,"sessionID":"ses_1","part":{"type":"text","text":"hello"}}`, wantCategory: agentwrap.EventMessage},
		{name: "reasoning", line: `{"type":"reasoning","sessionID":"ses_1"}`, wantCategory: agentwrap.EventMessage},
		{name: "tool", line: `{"type":"tool_use","sessionID":"ses_1"}`, wantCategory: agentwrap.EventTool},
		{name: "usage", line: `{"type":"usage.update","input_tokens":1,"output_tokens":2,"total_tokens":3}`, wantCategory: agentwrap.EventUsage},
		{name: "artifact", line: `{"type":"artifact.created","uri":"file:///tmp/a.txt"}`, wantCategory: agentwrap.EventArtifact},
		{name: "warning", line: `{"type":"runtime.warning","message":"careful"}`, wantCategory: agentwrap.EventWarning},
		{name: "permission", line: `{"type":"permission.asked","sessionID":"ses_1"}`, wantCategory: agentwrap.EventPermission},
		{name: "blocking", line: `{"type":"question.asked","sessionID":"ses_1"}`, wantCategory: agentwrap.EventBlocking},
		{name: "session", line: `{"type":"session.status","sessionID":"ses_1"}`, wantCategory: agentwrap.EventSession},
		{name: "final", line: `{"type":"step_finish","sessionID":"ses_1"}`, wantCategory: agentwrap.EventFinalResult, wantFinal: true},
		{name: "unknown", line: `{"type":"vendor.future","sessionID":"ses_1"}`, wantCategory: agentwrap.EventNativeExtension},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := decodeNativeLine([]byte(tt.line), int64(i+1))
			if err != nil {
				t.Fatal(err)
			}
			projected := projectNative(projectionInput{
				runID:  "run_1",
				seq:    int64(i + 1),
				record: record,
			})
			if projected.event.Category != tt.wantCategory {
				t.Fatalf("category = %s, want %s", projected.event.Category, tt.wantCategory)
			}
			if projected.final != tt.wantFinal {
				t.Fatalf("final = %v, want %v", projected.final, tt.wantFinal)
			}
			if projected.event.Raw == nil || projected.event.Raw.Safe {
				t.Fatalf("raw payload should exist and be unsafe: %#v", projected.event.Raw)
			}
		})
	}
}

func TestScanFixtureMalformed(t *testing.T) {
	data, err := os.ReadFile("testdata/malformed.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	err = scanNativeRecords(context.Background(), io.NopCloser(strings.NewReader(string(data))), func(nativeRecord) error { return nil })
	var d *decodeError
	if !errors.As(err, &d) {
		t.Fatalf("expected decodeError, got %T %v", err, err)
	}
	if d.line != 2 {
		t.Fatalf("line = %d, want 2", d.line)
	}
}

func TestScanBlankLineIsMalformed(t *testing.T) {
	err := scanNativeRecords(context.Background(), io.NopCloser(strings.NewReader("{\"type\":\"step_start\"}\n\n{\"type\":\"step_finish\"}\n")), func(nativeRecord) error { return nil })
	var d *decodeError
	if !errors.As(err, &d) {
		t.Fatalf("expected decodeError, got %T %v", err, err)
	}
	if d.line != 2 {
		t.Fatalf("line = %d, want 2", d.line)
	}
}

func TestScanRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := scanNativeRecords(ctx, io.NopCloser(strings.NewReader("")), func(nativeRecord) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %T %v, want context.Canceled", err, err)
	}
}
