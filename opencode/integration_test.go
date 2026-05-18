package opencode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antonioborgerees/agentwrap"
)

func TestRealOpenCodeSmoke(t *testing.T) {
	if os.Getenv("AGENTWRAP_OPENCODE_SMOKE") != "1" {
		t.Skip("set AGENTWRAP_OPENCODE_SMOKE=1 to run the real OpenCode smoke test")
	}
	run, err := NewRuntime().StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:  "Reply with one short sentence.",
		WorkDir: ".",
		Timeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range run.Events() {
	}
	result, err := run.Wait(context.Background())
	if err != nil {
		t.Fatalf("wait: %v result=%#v", err, result)
	}
	if result.Status != agentwrap.StateCompleted {
		t.Fatalf("status = %s", result.Status)
	}
}

func TestRealOpenCodeSmokeSuite(t *testing.T) {
	if os.Getenv("AGENTWRAP_OPENCODE_SMOKE_SUITE") != "1" {
		t.Skip("set AGENTWRAP_OPENCODE_SMOKE_SUITE=1 to run the extended real OpenCode smoke suite")
	}

	model := smokeModel(t)
	provider, modelID := splitSmokeModel(model)
	configPath := os.Getenv("AGENTWRAP_OPENCODE_SMOKE_CONFIG")
	var baseOptions []Option
	if configPath != "" {
		baseOptions = append(baseOptions, WithEnv("OPENCODE_CONFIG="+configPath))
	}

	t.Run("basic final text", func(t *testing.T) {
		events, result, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:   "Reply with exactly: agentwrap smoke ok",
			WorkDir:  ".",
			Provider: agentwrap.ProviderID(provider),
			Model:    agentwrap.ModelID(modelID),
			Timeout:  2 * time.Minute,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))...)
		if err != nil {
			t.Fatalf("wait: %v result=%#v", err, result)
		}
		requireCompleted(t, result)
		requireEventCategory(t, events, agentwrap.EventProgress)
		requireEventCategory(t, events, agentwrap.EventMessage)
		requireEventCategory(t, events, agentwrap.EventFinalResult)
		requireUnsafeRawPayloads(t, events)
	})

	t.Run("file output", func(t *testing.T) {
		dir := t.TempDir()
		output := filepath.Join(dir, "agentwrap-smoke-output.txt")
		events, result, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:   "Use the shell to run exactly: printf 'agentwrap smoke ok\n' > agentwrap-smoke-output.txt. Then reply with exactly: file smoke done",
			WorkDir:  dir,
			Provider: agentwrap.ProviderID(provider),
			Model:    agentwrap.ModelID(modelID),
			Timeout:  3 * time.Minute,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))...)
		if err != nil {
			t.Fatalf("wait: %v result=%#v", err, result)
		}
		requireCompleted(t, result)
		requireEventCategory(t, events, agentwrap.EventFinalResult)
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatalf("expected OpenCode to create %s: %v\nevents:\n%s", output, err, summarizeEvents(events))
		}
		if !strings.Contains(string(data), "agentwrap smoke ok") {
			t.Fatalf("unexpected file content: %q", string(data))
		}
	})

	t.Run("same session continuation", func(t *testing.T) {
		token := fmt.Sprintf("agentwrap-session-token-%d", time.Now().UnixNano())
		events1, result1, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:        fmt.Sprintf("Remember this exact token for the next turn: %s. Reply exactly: memory stored", token),
			WorkDir:       ".",
			Provider:      agentwrap.ProviderID(provider),
			Model:         agentwrap.ModelID(modelID),
			Timeout:       4 * time.Minute,
			WantSession:   true,
			SessionAction: agentwrap.SessionActionFresh,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))...)
		if err != nil {
			t.Fatalf("first wait: %v result=%#v", err, result1)
		}
		requireCompleted(t, result1)
		if result1.SessionID == "" {
			t.Fatalf("first run did not produce session id: %#v", result1)
		}
		requireSessionRelationship(t, result1, agentwrap.SessionRelationshipFresh)
		requireEventCategory(t, events1, agentwrap.EventSession)

		events2, result2, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:        "Reply with exactly the secret token from the previous turn and nothing else.",
			WorkDir:       ".",
			Provider:      agentwrap.ProviderID(provider),
			Model:         agentwrap.ModelID(modelID),
			Timeout:       4 * time.Minute,
			SessionID:     result1.SessionID,
			SessionAction: agentwrap.SessionActionContinue,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))...)
		if err != nil {
			t.Fatalf("second wait: %v result=%#v", err, result2)
		}
		requireCompleted(t, result2)
		if result2.Metadata.Session.RequestedID != result1.SessionID {
			t.Fatalf("requested session id = %q, want %q", result2.Metadata.Session.RequestedID, result1.SessionID)
		}
		requireSessionRelationship(t, result2, agentwrap.SessionRelationshipBestEffort)
		requireEventCategory(t, events2, agentwrap.EventSession)
		if !eventsContainText(events2, token) {
			t.Fatalf("continuation token %q not observed in second-run events:\n%s", token, summarizeEvents(events2))
		}
	})

	t.Run("invalid model fails with classified runtime exit", func(t *testing.T) {
		_, result, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:   "Reply with one short sentence.",
			WorkDir:  ".",
			Provider: "agentwrap-missing-provider",
			Model:    "agentwrap-missing-model",
			Timeout:  1 * time.Minute,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions"))...)
		requireSDKError(t, err, result, agentwrap.ErrorRuntimeExit)
		if result.Status != agentwrap.StateFailed {
			t.Fatalf("status = %s, want failed", result.Status)
		}
		if exitCode, _ := result.Metadata.NativeMetadata["exit_code"].(int); exitCode == 0 {
			t.Fatalf("expected non-zero exit metadata: %#v", result.Metadata.NativeMetadata)
		}
	})

	t.Run("timeout fails with classified timeout", func(t *testing.T) {
		_, result, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:   "Wait briefly, then reply with one sentence.",
			WorkDir:  ".",
			Provider: agentwrap.ProviderID(provider),
			Model:    agentwrap.ModelID(modelID),
			Timeout:  500 * time.Millisecond,
		}, append(baseOptions, WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))...)
		requireSDKError(t, err, result, agentwrap.ErrorTimeout)
	})

	t.Run("ultraplan config parity", func(t *testing.T) {
		path := configPath
		if path == "" {
			path = filepath.Clean("/home/antonioborgerees/coding/ultraplan/cli/opencode-config.json")
		}
		if _, err := os.Stat(path); err != nil {
			t.Skipf("OpenCode config not available: %s", path)
		}
		events, result, err := runRealSmoke(t, agentwrap.RunRequest{
			Prompt:   "Reply with exactly: agentwrap config smoke ok",
			WorkDir:  ".",
			Provider: agentwrap.ProviderID(provider),
			Model:    agentwrap.ModelID(modelID),
			Timeout:  2 * time.Minute,
		}, WithEnv("OPENCODE_CONFIG="+path), WithExtraArgs("--dangerously-skip-permissions", "--variant", smokeVariant()))
		if err != nil {
			t.Fatalf("wait: %v result=%#v", err, result)
		}
		requireCompleted(t, result)
		requireEventCategory(t, events, agentwrap.EventFinalResult)
	})
}

func runRealSmoke(t *testing.T, req agentwrap.RunRequest, options ...Option) ([]agentwrap.Event, agentwrap.RunResult, error) {
	t.Helper()
	run, err := NewRuntime(options...).StartRun(context.Background(), req)
	if err != nil {
		return nil, agentwrap.RunResult{}, err
	}
	var events []agentwrap.Event
	for event := range run.Events() {
		events = append(events, event)
	}
	result, err := run.Wait(context.Background())
	return events, result, err
}

func smokeModel(t *testing.T) string {
	t.Helper()
	model := os.Getenv("AGENTWRAP_OPENCODE_SMOKE_MODEL")
	if model == "" {
		model = "openai/gpt-5.5"
	}
	if !strings.Contains(model, "/") {
		t.Fatalf("AGENTWRAP_OPENCODE_SMOKE_MODEL must use provider/model format, got %q", model)
	}
	return model
}

func smokeVariant() string {
	variant := os.Getenv("AGENTWRAP_OPENCODE_SMOKE_VARIANT")
	if variant == "" {
		return "low"
	}
	return variant
}

func splitSmokeModel(model string) (string, string) {
	provider, rest, _ := strings.Cut(model, "/")
	return provider, rest
}

func requireCompleted(t *testing.T, result agentwrap.RunResult) {
	t.Helper()
	if result.Err != nil || result.Status != agentwrap.StateCompleted {
		t.Fatalf("result = %#v err=%v", result, result.Err)
	}
}

func requireEventCategory(t *testing.T, events []agentwrap.Event, category agentwrap.EventCategory) {
	t.Helper()
	for _, event := range events {
		if event.Category == category {
			return
		}
	}
	t.Fatalf("missing event category %s in %#v", category, events)
}

func requireUnsafeRawPayloads(t *testing.T, events []agentwrap.Event) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("expected events")
	}
	for _, event := range events {
		if event.Raw == nil {
			t.Fatalf("event %s missing raw payload", event.ID)
		}
		if event.Raw.Safe {
			t.Fatalf("event %s raw payload unexpectedly safe", event.ID)
		}
	}
}

func requireSDKError(t *testing.T, err error, result agentwrap.RunResult, category agentwrap.ErrorCategory) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s error, got nil result=%#v", category, result)
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) {
		t.Fatalf("expected SDKError, got %T %v", err, err)
	}
	if sdkErr.Category != category {
		t.Fatalf("category = %s, want %s result=%#v", sdkErr.Category, category, result)
	}
	if result.Err == nil || result.Err.Category != category {
		t.Fatalf("result error = %#v, want %s", result.Err, category)
	}
}

func requireSessionRelationship(t *testing.T, result agentwrap.RunResult, relationship agentwrap.SessionRelationship) {
	t.Helper()
	if result.Metadata.Session.Relationship != relationship {
		t.Fatalf("session relationship = %q, want %q result=%#v", result.Metadata.Session.Relationship, relationship, result)
	}
}

func eventsContainText(events []agentwrap.Event, want string) bool {
	for _, event := range events {
		if strings.Contains(textFromEvent(event), want) {
			return true
		}
	}
	return false
}

func summarizeEvents(events []agentwrap.Event) string {
	var b strings.Builder
	for _, event := range events {
		b.WriteString(string(event.Category))
		b.WriteString(" ")
		b.WriteString(event.Type)
		if text := textFromEvent(event); text != "" {
			b.WriteString(" ")
			b.WriteString(text)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func textFromEvent(event agentwrap.Event) string {
	part, _ := event.Payload["part"].(map[string]any)
	if part == nil {
		return ""
	}
	text, _ := part["text"].(string)
	return strings.TrimSpace(text)
}
