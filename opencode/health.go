package opencode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/antonioborgerees/agentwrap"
)

var _ agentwrap.HealthChecker = (*Runtime)(nil)

// CheckHealth runs cheap OpenCode preflight probes without starting agent work.
func (r *Runtime) CheckHealth(ctx context.Context, req agentwrap.HealthCheckRequest) (agentwrap.HealthReport, error) {
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	if req.Context.RuntimeKind == "" {
		req.Context.RuntimeKind = agentwrap.RuntimeKind("opencode")
	}
	if req.Context.RuntimeName == "" {
		req.Context.RuntimeName = "opencode"
	}
	if req.Provider != "" {
		req.Context.Provider = req.Provider
	}
	if req.Model != "" {
		req.Context.Model = req.Model
	}
	report := agentwrap.HealthReport{
		Context:         req.Context,
		EffectiveConfig: r.effectiveConfig(req),
		GeneratedAt:     r.now(),
		NativeMetadata: map[string]any{
			"env": agentwrap.RedactMetadata(map[string]any{"values": strings.Join(agentwrap.RedactEnv(r.env), "\n")})["values"],
		},
	}
	if err := agentwrap.ValidateEffectiveConfig(report.EffectiveConfig); err != nil {
		report.Results = append(report.Results, healthResult(r.now, agentwrap.HealthCheckConfig, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, nil, err))
		return agentwrap.AggregateHealth(report), nil
	}
	checks := req.Checks
	if len(checks) == 0 {
		checks = []agentwrap.HealthCheckID{
			agentwrap.HealthCheckRuntimeAvailable,
			agentwrap.HealthCheckStructuredOutput,
			agentwrap.HealthCheckWorkDir,
			agentwrap.HealthCheckConfig,
			agentwrap.HealthCheckRuntimePaths,
			agentwrap.HealthCheckProvider,
			agentwrap.HealthCheckAuthentication,
			agentwrap.HealthCheckModel,
		}
	}
	for _, check := range checks {
		select {
		case <-ctx.Done():
			err := agentwrap.NewError(agentwrap.ErrorTimeout, "opencode health", "OpenCode health check timed out", ctx.Err())
			report.Results = append(report.Results, healthResult(r.now, check, agentwrap.HealthTransientFail, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, nil, err))
			return agentwrap.AggregateHealth(report), nil
		default:
		}
		report.Results = append(report.Results, r.runHealthCheck(ctx, req, check))
	}
	return agentwrap.AggregateHealth(report), nil
}

func (r *Runtime) runHealthCheck(ctx context.Context, req agentwrap.HealthCheckRequest, check agentwrap.HealthCheckID) agentwrap.HealthResult {
	switch check {
	case agentwrap.HealthCheckRuntimeAvailable:
		return r.probeCommand(ctx, check, []string{"--version"}, agentwrap.ErrorRuntimeUnavailable, "OpenCode executable is available")
	case agentwrap.HealthCheckStructuredOutput:
		result := r.probeCommand(ctx, check, []string{"run", "--help"}, agentwrap.ErrorHealth, "OpenCode run help is available")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		text := fmt.Sprint(result.NativeMetadata["stdout"]) + "\n" + fmt.Sprint(result.NativeMetadata["stderr"])
		if !strings.Contains(text, "--format") || !strings.Contains(text, "json") {
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorHealth, "OpenCode structured JSON output support was not detected", text, nil)
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, result.NativeMetadata, err)
		}
		return result
	case agentwrap.HealthCheckWorkDir:
		if req.WorkDir == "" {
			return healthResult(r.now, check, agentwrap.HealthSkipped, agentwrap.HealthSeverityInfo, "no working directory was provided", "", nil, nil)
		}
		info, err := os.Stat(req.WorkDir)
		if err != nil || !info.IsDir() {
			if err == nil {
				err = errors.New("workdir is not a directory")
			}
			sdkErr := agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode workdir", "working directory is invalid", err, agentwrap.WithDebugDetail(req.WorkDir), agentwrap.WithUserActionable(true), agentwrap.WithUnrecoverable(true))
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, sdkErr.UserDetail, sdkErr.DebugDetail, nil, sdkErr)
		}
		return healthResult(r.now, check, agentwrap.HealthReady, agentwrap.HealthSeverityInfo, "working directory is valid", "", nil, nil)
	case agentwrap.HealthCheckConfig:
		return r.probeCommand(ctx, check, []string{"debug", "config"}, agentwrap.ErrorHealth, "OpenCode configuration probe completed")
	case agentwrap.HealthCheckRuntimePaths:
		return r.probeCommand(ctx, check, []string{"debug", "paths"}, agentwrap.ErrorHealth, "OpenCode paths probe completed")
	case agentwrap.HealthCheckProvider, agentwrap.HealthCheckAuthentication:
		if req.Provider == "" {
			return healthResult(r.now, check, agentwrap.HealthSkipped, agentwrap.HealthSeverityInfo, "no provider was requested", "", nil, nil)
		}
		result := r.probeCommand(ctx, check, []string{"providers", "list"}, agentwrap.ErrorProviderUnavailable, "OpenCode provider probe completed")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		output := strings.ToLower(fmt.Sprint(result.NativeMetadata["stdout"]))
		provider := strings.ToLower(string(req.Provider))
		if !strings.Contains(output, provider) {
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorProviderUnavailable, "requested provider was not found", provider, nil)
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, result.NativeMetadata, err)
		}
		if check == agentwrap.HealthCheckAuthentication && !strings.Contains(output, "auth") && !strings.Contains(output, "login") && !strings.Contains(output, "key") {
			return healthResult(r.now, check, agentwrap.HealthUnknown, agentwrap.HealthSeverityWarn, "provider authentication readiness could not be proven without starting work", "", result.NativeMetadata, nil)
		}
		return result
	case agentwrap.HealthCheckModel:
		if req.Provider == "" || req.Model == "" {
			return healthResult(r.now, check, agentwrap.HealthSkipped, agentwrap.HealthSeverityInfo, "provider or model was not requested", "", nil, nil)
		}
		args := []string{"models", string(req.Provider), "--verbose"}
		if req.IncludeRefresh {
			args = append(args, "--refresh")
		}
		result := r.probeCommand(ctx, check, args, agentwrap.ErrorModelUnavailable, "OpenCode model probe completed")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		output := strings.ToLower(fmt.Sprint(result.NativeMetadata["stdout"]))
		model := strings.ToLower(string(req.Model))
		if !strings.Contains(output, model) {
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorModelUnavailable, "requested model was not found", model, nil)
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, result.NativeMetadata, err)
		}
		return result
	default:
		err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnsupported, agentwrap.ErrorHealth, "health check is unsupported", string(check), nil)
		return healthResult(r.now, check, agentwrap.HealthUnsupported, agentwrap.HealthSeverityWarn, err.UserDetail, err.DebugDetail, nil, err)
	}
}

func (r *Runtime) probeCommand(ctx context.Context, check agentwrap.HealthCheckID, args []string, category agentwrap.ErrorCategory, readyDetail string) agentwrap.HealthResult {
	started := r.now()
	proc, err := r.runner.Start(ctx, processSpec{Executable: r.executable, Args: args, Env: r.env})
	if err != nil {
		sdkErr := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, category, "OpenCode probe could not start", err.Error(), err)
		return agentwrap.HealthResult{Check: check, Status: agentwrap.HealthUnrecoverable, Severity: agentwrap.HealthSeverityError, UserDetail: sdkErr.UserDetail, DebugDetail: sdkErr.DebugDetail, Err: sdkErr, StartedAt: started, FinishedAt: r.now()}
	}
	stdoutCh := make(chan string, 1)
	stderrCh := make(chan string, 1)
	go func() {
		stdoutCh <- readProbeOutput(proc.Stdout(), r.stderrLimit)
	}()
	go func() {
		stderrCh <- readProbeOutput(io.NopCloser(proc.Stderr()), r.stderrLimit)
	}()
	result := proc.Wait()
	stdout := <-stdoutCh
	stderr := <-stderrCh
	native := map[string]any{
		"args":      append([]string(nil), args...),
		"stdout":    agentwrap.RedactString(stdout),
		"stderr":    agentwrap.RedactString(stderr),
		"exit_code": result.ExitCode,
	}
	if result.Err != nil || result.ExitCode != 0 {
		detail := fmt.Sprintf("exit_code=%d stderr=%s", result.ExitCode, agentwrap.RedactString(stderr))
		sdkErr := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthTransientFail, category, "OpenCode probe failed", detail, result.Err)
		return healthResult(r.now, check, agentwrap.HealthTransientFail, agentwrap.HealthSeverityError, sdkErr.UserDetail, sdkErr.DebugDetail, native, sdkErr)
	}
	return agentwrap.HealthResult{Check: check, Status: agentwrap.HealthReady, Severity: agentwrap.HealthSeverityInfo, UserDetail: readyDetail, NativeMetadata: native, StartedAt: started, FinishedAt: r.now()}
}

func readProbeOutput(reader io.ReadCloser, limit int) string {
	defer reader.Close()
	if limit <= 0 {
		_, _ = io.Copy(io.Discard, reader)
		return ""
	}
	limited := newLimitBuffer(limit)
	_, _ = io.Copy(io.MultiWriter(limited, io.Discard), reader)
	return limited.String()
}

func healthResult(now clock, check agentwrap.HealthCheckID, status agentwrap.HealthStatus, severity agentwrap.HealthSeverity, userDetail, debugDetail string, native map[string]any, err *agentwrap.SDKError) agentwrap.HealthResult {
	t := now()
	return agentwrap.HealthResult{
		Check:          check,
		Status:         status,
		Severity:       severity,
		UserDetail:     userDetail,
		DebugDetail:    agentwrap.RedactString(debugDetail),
		NativeMetadata: agentwrap.RedactMetadata(native),
		Err:            err,
		StartedAt:      t,
		FinishedAt:     t,
	}
}

func (r *Runtime) effectiveConfig(req agentwrap.HealthCheckRequest) agentwrap.EffectiveConfig {
	runtimeName := "opencode"
	defaultLayer := agentwrap.ConfigLayer{
		Source:      agentwrap.ConfigSourceDefault,
		RuntimeName: &runtimeName,
	}
	adapterLayer := agentwrap.ConfigLayer{Source: agentwrap.ConfigSourceAdapterOption, Executable: &r.executable}
	for _, env := range r.env {
		if secret, ok := agentwrap.SecretFromEnv(env, agentwrap.ConfigSourceAdapterOption); ok {
			adapterLayer.Secrets = append(adapterLayer.Secrets, secret)
		}
	}
	caller := agentwrap.ConfigLayer{Source: agentwrap.ConfigSourceCallerRequest, Metadata: req.Metadata}
	if req.Provider != "" {
		caller.Provider = &req.Provider
	}
	if req.Model != "" {
		caller.Model = &req.Model
	}
	if req.WorkDir != "" {
		caller.WorkDir = &req.WorkDir
	}
	if req.Permissions != "" {
		caller.Permissions = &req.Permissions
	}
	if req.Sandbox != "" {
		caller.Sandbox = &req.Sandbox
	}
	if req.Timeout != 0 {
		caller.Timeout = &req.Timeout
	}
	return agentwrap.MergeEffectiveConfig(agentwrap.RuntimeKind("opencode"), defaultLayer, adapterLayer, caller)
}
