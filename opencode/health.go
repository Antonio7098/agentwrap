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
		return r.probeCommand(ctx, check, req.WorkDir, []string{"--version"}, nil, agentwrap.ErrorRuntimeUnavailable, "OpenCode executable is available")
	case agentwrap.HealthCheckStructuredOutput:
		result := r.probeCommand(ctx, check, req.WorkDir, []string{"run", "--help"}, []string{"--format", "json"}, agentwrap.ErrorHealth, "OpenCode run help is available")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		if !probeMatched(result, "--format") || !probeMatched(result, "json") {
			detail := fmt.Sprint(result.NativeMetadata["stdout"]) + "\n" + fmt.Sprint(result.NativeMetadata["stderr"])
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorHealth, "OpenCode structured JSON output support was not detected", detail, nil)
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
			sdkErr := agentwrap.NewError(agentwrap.ErrorConfiguration, "opencode workdir", "working directory is invalid", err, agentwrap.WithDebugDetail(req.WorkDir))
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, sdkErr.UserDetail, sdkErr.DebugDetail, nil, sdkErr)
		}
		return healthResult(r.now, check, agentwrap.HealthReady, agentwrap.HealthSeverityInfo, "working directory is valid", "", nil, nil)
	case agentwrap.HealthCheckConfig:
		return r.probeCommand(ctx, check, req.WorkDir, []string{"debug", "config"}, nil, agentwrap.ErrorHealth, "OpenCode configuration probe completed")
	case agentwrap.HealthCheckRuntimePaths:
		return r.probeCommand(ctx, check, req.WorkDir, []string{"debug", "paths"}, nil, agentwrap.ErrorHealth, "OpenCode paths probe completed")
	case agentwrap.HealthCheckProvider, agentwrap.HealthCheckAuthentication:
		if req.Provider == "" {
			return healthResult(r.now, check, agentwrap.HealthSkipped, agentwrap.HealthSeverityInfo, "no provider was requested", "", nil, nil)
		}
		provider := strings.ToLower(string(req.Provider))
		result := r.probeCommand(ctx, check, req.WorkDir, []string{"providers", "list"}, []string{provider}, agentwrap.ErrorProviderUnavailable, "OpenCode provider probe completed")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		output := strings.ToLower(fmt.Sprint(result.NativeMetadata["stdout"]))
		providerLine := ""
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, provider) {
				providerLine = line
				break
			}
		}
		if providerLine == "" && !probeMatched(result, provider) {
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorProviderUnavailable, "requested provider was not found", provider, nil)
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, result.NativeMetadata, err)
		}
		if check == agentwrap.HealthCheckAuthentication &&
			(providerLine == "" ||
				(!strings.Contains(providerLine, "auth") &&
					!strings.Contains(providerLine, "login") &&
					!strings.Contains(providerLine, "key"))) {
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
		model := strings.ToLower(string(req.Model))
		result := r.probeCommand(ctx, check, req.WorkDir, args, []string{model}, agentwrap.ErrorModelUnavailable, "OpenCode model probe completed")
		if result.Status != agentwrap.HealthReady {
			return result
		}
		output := strings.ToLower(fmt.Sprint(result.NativeMetadata["stdout"]))
		if !strings.Contains(output, model) && !probeMatched(result, model) {
			err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, agentwrap.ErrorModelUnavailable, "requested model was not found", model, nil)
			return healthResult(r.now, check, agentwrap.HealthUnrecoverable, agentwrap.HealthSeverityError, err.UserDetail, err.DebugDetail, result.NativeMetadata, err)
		}
		return result
	default:
		err := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnsupported, agentwrap.ErrorHealth, "health check is unsupported", string(check), nil)
		return healthResult(r.now, check, agentwrap.HealthUnsupported, agentwrap.HealthSeverityWarn, err.UserDetail, err.DebugDetail, nil, err)
	}
}

func (r *Runtime) probeCommand(ctx context.Context, check agentwrap.HealthCheckID, workDir string, args []string, needles []string, category agentwrap.ErrorCategory, readyDetail string) agentwrap.HealthResult {
	started := r.now()
	proc, err := r.runner.Start(ctx, processSpec{Executable: r.executable, Args: args, Env: r.env, WorkDir: workDir})
	if err != nil {
		sdkErr := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthUnrecoverable, category, "OpenCode probe could not start", err.Error(), err)
		return agentwrap.HealthResult{Check: check, Status: agentwrap.HealthUnrecoverable, Severity: agentwrap.HealthSeverityError, UserDetail: sdkErr.UserDetail, DebugDetail: sdkErr.DebugDetail, Err: sdkErr, StartedAt: started, FinishedAt: r.now()}
	}
	stdoutCh := make(chan probeOutput, 1)
	stderrCh := make(chan probeOutput, 1)
	go func() {
		stdoutCh <- readProbeOutput(proc.Stdout(), r.stderrLimit, needles)
	}()
	go func() {
		stderrCh <- readProbeOutput(io.NopCloser(proc.Stderr()), r.stderrLimit, needles)
	}()
	result := proc.Wait()
	stdout := <-stdoutCh
	stderr := <-stderrCh
	matches := mergeProbeMatches(stdout.Matches, stderr.Matches)
	native := map[string]any{
		"args":      append([]string(nil), args...),
		"workdir":   workDir,
		"stdout":    agentwrap.RedactString(stdout.Sample),
		"stderr":    agentwrap.RedactString(stderr.Sample),
		"matches":   matches,
		"exit_code": result.ExitCode,
	}
	if result.Err != nil || result.ExitCode != 0 {
		detail := fmt.Sprintf("exit_code=%d stderr=%s", result.ExitCode, agentwrap.RedactString(stderr.Sample))
		sdkErr := agentwrap.ErrorForHealthStatus(check, agentwrap.HealthTransientFail, category, "OpenCode probe failed", detail, result.Err)
		return healthResult(r.now, check, agentwrap.HealthTransientFail, agentwrap.HealthSeverityError, sdkErr.UserDetail, sdkErr.DebugDetail, native, sdkErr)
	}
	return agentwrap.HealthResult{Check: check, Status: agentwrap.HealthReady, Severity: agentwrap.HealthSeverityInfo, UserDetail: readyDetail, NativeMetadata: native, StartedAt: started, FinishedAt: r.now()}
}

type probeOutput struct {
	Sample  string
	Matches map[string]bool
}

func readProbeOutput(reader io.ReadCloser, limit int, needles []string) probeOutput {
	defer reader.Close()
	matches := make(map[string]bool, len(needles))
	if limit <= 0 {
		_, _ = scanProbeOutput(io.Discard, reader, needles, matches)
		return probeOutput{Matches: matches}
	}
	limited := newLimitBuffer(limit)
	_, _ = scanProbeOutput(limited, reader, needles, matches)
	return probeOutput{Sample: limited.String(), Matches: matches}
}

func scanProbeOutput(sample io.Writer, reader io.Reader, needles []string, matches map[string]bool) (int64, error) {
	lowerNeedles := make([]string, 0, len(needles))
	maxNeedle := 0
	for _, needle := range needles {
		needle = strings.ToLower(needle)
		if needle == "" {
			continue
		}
		lowerNeedles = append(lowerNeedles, needle)
		if len(needle) > maxNeedle {
			maxNeedle = len(needle)
		}
	}
	buf := make([]byte, 4096)
	var total int64
	tail := ""
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			total += int64(n)
			chunk := string(buf[:n])
			_, _ = sample.Write(buf[:n])
			window := tail + strings.ToLower(chunk)
			for _, needle := range lowerNeedles {
				if strings.Contains(window, needle) {
					matches[needle] = true
				}
			}
			if maxNeedle > 1 && len(window) >= maxNeedle-1 {
				tail = window[len(window)-(maxNeedle-1):]
			} else {
				tail = window
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

func mergeProbeMatches(parts ...map[string]bool) map[string]bool {
	matches := map[string]bool{}
	for _, part := range parts {
		for key, value := range part {
			matches[key] = matches[key] || value
		}
	}
	return matches
}

func probeMatched(result agentwrap.HealthResult, needle string) bool {
	matches, _ := result.NativeMetadata["matches"].(map[string]bool)
	return matches[strings.ToLower(needle)]
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
	caller := agentwrap.ConfigLayer{Source: agentwrap.ConfigSourceCallerRequest, Metadata: agentwrap.RedactStringMap(req.Metadata)}
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
