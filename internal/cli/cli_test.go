package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunUsesInjectedIO(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := Run(Config{
		Args:    []string{"--version"},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "test-version",
	})

	if code != 0 {
		t.Fatalf("Run returned %d, want 0", code)
	}
	if got := stdout.String(); got != "agentwrap test-version\n" {
		t.Fatalf("stdout = %q", got)
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestRunCanBeInvokedRepeatedlyWithDifferentBuffers(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "help", args: []string{"--help"}, wantCode: 0, wantStdout: "Usage:"},
		{name: "unknown", args: []string{"run"}, wantCode: 2, wantStderr: `unknown command "run"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer

			code := Run(Config{Args: tc.args, Stdout: &stdout, Stderr: &stderr})

			if code != tc.wantCode {
				t.Fatalf("Run returned %d, want %d", code, tc.wantCode)
			}
			if tc.wantStdout != "" && !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Fatalf("stdout = %q, want it to contain %q", stdout.String(), tc.wantStdout)
			}
			if tc.wantStderr != "" && !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}
