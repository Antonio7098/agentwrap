package opencode

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Antonio7098/agentwrap"
)

func TestCheckHealthUsesFakeProbesAndRedactsSecrets(t *testing.T) {
	runner := &fakeRunner{procs: []process{
		&fakeProcess{stdout: "opencode 1.0\n"},
		&fakeProcess{stdout: "usage: opencode run --format json\n"},
		&fakeProcess{stdout: "anthropic authenticated\n"},
		&fakeProcess{stdout: "claude-sonnet\n"},
	}}
	rt := NewRuntime(withProcessRunner(runner), WithEnv("ANTHROPIC_API_KEY=secret"))
	report, err := rt.CheckHealth(context.Background(), agentwrap.HealthCheckRequest{
		Provider: "anthropic",
		Model:    "claude-sonnet",
		WorkDir:  "/tmp/agentwrap-health-workdir",
		Metadata: map[string]string{
			"api_key": "raw-secret",
			"note":    "Bearer metadata-token",
		},
		Checks: []agentwrap.HealthCheckID{
			agentwrap.HealthCheckRuntimeAvailable,
			agentwrap.HealthCheckStructuredOutput,
			agentwrap.HealthCheckProvider,
			agentwrap.HealthCheckModel,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.OverallStatus != agentwrap.HealthReady {
		t.Fatalf("status = %s report=%#v", report.OverallStatus, report)
	}
	if runner.starts != 4 {
		t.Fatalf("starts = %d, want 4", runner.starts)
	}
	if len(report.EffectiveConfig.Secrets) != 1 || report.EffectiveConfig.Secrets[0].Name != "ANTHROPIC_API_KEY" {
		t.Fatalf("secrets = %#v", report.EffectiveConfig.Secrets)
	}
	if env := report.NativeMetadata["env"].(string); contains(env, "secret") {
		t.Fatalf("secret leaked in env metadata: %q", env)
	}
	if got := report.EffectiveConfig.Metadata["api_key"].Value; got != "[REDACTED]" {
		t.Fatalf("api key metadata = %q", got)
	}
	if contains(report.EffectiveConfig.Metadata["note"].Value, "metadata-token") {
		t.Fatalf("metadata secret leaked: %#v", report.EffectiveConfig.Metadata["note"])
	}
	if runner.spec.WorkDir != "/tmp/agentwrap-health-workdir" {
		t.Fatalf("probe workdir = %q", runner.spec.WorkDir)
	}
}

func TestCheckHealthDetectsStructuredOutputBeyondDiagnosticSample(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: strings.Repeat("x", 128) + " --format json\n"}}
	rt := NewRuntime(withProcessRunner(runner), WithStderrLimit(16))
	report, err := rt.CheckHealth(context.Background(), agentwrap.HealthCheckRequest{
		Checks: []agentwrap.HealthCheckID{agentwrap.HealthCheckStructuredOutput},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Results[0].Status != agentwrap.HealthReady {
		t.Fatalf("status = %s result=%#v", report.Results[0].Status, report.Results[0])
	}
}

func TestStartRunRequiredPreflightBlocksProcessStart(t *testing.T) {
	runner := &fakeRunner{err: errors.New("missing opencode")}
	rt := NewRuntime(withProcessRunner(runner))
	_, err := rt.StartRun(context.Background(), agentwrap.RunRequest{
		Prompt:        "hello",
		RequireHealth: []agentwrap.HealthCheckID{agentwrap.HealthCheckRuntimeAvailable},
	})
	if err == nil {
		t.Fatal("expected preflight error")
	}
	var sdkErr *agentwrap.SDKError
	if !errors.As(err, &sdkErr) || sdkErr.Category != agentwrap.ErrorRuntimeUnavailable {
		t.Fatalf("err = %#v", err)
	}
	if runner.starts != 1 {
		t.Fatalf("starts = %d, want only preflight probe start", runner.starts)
	}
}

func TestCheckHealthReportsUnknownAuthenticationWhenUnproven(t *testing.T) {
	runner := &fakeRunner{proc: &fakeProcess{stdout: "openai authenticated\nanthropic\n"}}
	rt := NewRuntime(withProcessRunner(runner))
	report, err := rt.CheckHealth(context.Background(), agentwrap.HealthCheckRequest{
		Provider: "anthropic",
		Checks:   []agentwrap.HealthCheckID{agentwrap.HealthCheckAuthentication},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Results[0].Status != agentwrap.HealthUnknown {
		t.Fatalf("auth status = %s, want unknown", report.Results[0].Status)
	}
}

func TestCheckHealthClassifiesProbeFailuresAndUnavailableProviderModel(t *testing.T) {
	tests := []struct {
		name       string
		check      agentwrap.HealthCheckID
		proc       process
		provider   agentwrap.ProviderID
		model      agentwrap.ModelID
		wantStatus agentwrap.HealthStatus
		wantCat    agentwrap.ErrorCategory
	}{
		{
			name:       "debug config failure",
			check:      agentwrap.HealthCheckConfig,
			proc:       &fakeProcess{stderr: "token=secret", result: processResult{ExitCode: 2, Err: errors.New("exit status 2")}},
			wantStatus: agentwrap.HealthTransientFail,
			wantCat:    agentwrap.ErrorHealth,
		},
		{
			name:       "provider missing",
			check:      agentwrap.HealthCheckProvider,
			proc:       &fakeProcess{stdout: "openai\n"},
			provider:   "anthropic",
			wantStatus: agentwrap.HealthUnrecoverable,
			wantCat:    agentwrap.ErrorProviderUnavailable,
		},
		{
			name:       "model missing",
			check:      agentwrap.HealthCheckModel,
			proc:       &fakeProcess{stdout: "haiku\n"},
			provider:   "anthropic",
			model:      "sonnet",
			wantStatus: agentwrap.HealthUnrecoverable,
			wantCat:    agentwrap.ErrorModelUnavailable,
		},
		{
			name:       "structured output unsupported",
			check:      agentwrap.HealthCheckStructuredOutput,
			proc:       &fakeProcess{stdout: "usage: opencode run\n"},
			wantStatus: agentwrap.HealthUnrecoverable,
			wantCat:    agentwrap.ErrorHealth,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &fakeRunner{proc: tt.proc}
			rt := NewRuntime(withProcessRunner(runner))
			report, err := rt.CheckHealth(context.Background(), agentwrap.HealthCheckRequest{
				Provider: tt.provider,
				Model:    tt.model,
				Checks:   []agentwrap.HealthCheckID{tt.check},
			})
			if err != nil {
				t.Fatal(err)
			}
			result := report.Results[0]
			if result.Status != tt.wantStatus {
				t.Fatalf("status = %s, want %s", result.Status, tt.wantStatus)
			}
			if result.Err == nil || result.Err.Category != tt.wantCat {
				t.Fatalf("err = %#v, want category %s", result.Err, tt.wantCat)
			}
			if result.DebugDetail != "" && contains(result.DebugDetail, "secret") {
				t.Fatalf("secret leaked: %q", result.DebugDetail)
			}
		})
	}
}

func contains(value, needle string) bool {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
