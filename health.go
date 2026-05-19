package agentwrap

import (
	"context"
	"time"
)

// HealthChecker runs preflight checks without starting runtime work.
type HealthChecker interface {
	CheckHealth(context.Context, HealthCheckRequest) (HealthReport, error)
}

// HealthCheckID identifies a runtime-neutral or adapter-specific preflight check.
type HealthCheckID string

const (
	HealthCheckRuntimeAvailable HealthCheckID = "runtime_available"
	HealthCheckStructuredOutput HealthCheckID = "structured_output"
	HealthCheckWorkDir          HealthCheckID = "workdir"
	HealthCheckConfig           HealthCheckID = "config"
	HealthCheckProvider         HealthCheckID = "provider"
	HealthCheckModel            HealthCheckID = "model"
	HealthCheckAuthentication   HealthCheckID = "authentication"
	HealthCheckRuntimePaths     HealthCheckID = "runtime_paths"
)

// HealthStatus is the caller-facing result for a single preflight check.
type HealthStatus string

const (
	HealthReady         HealthStatus = "ready"
	HealthDegraded      HealthStatus = "degraded"
	HealthTransientFail HealthStatus = "transient_failure"
	HealthUnrecoverable HealthStatus = "unrecoverable_failure"
	HealthUnknown       HealthStatus = "unknown"
	HealthSkipped       HealthStatus = "skipped"
	HealthUnsupported   HealthStatus = "unsupported"
)

// HealthSeverity gives policy code a coarse ordering without string matching.
type HealthSeverity string

const (
	HealthSeverityInfo  HealthSeverity = "info"
	HealthSeverityWarn  HealthSeverity = "warn"
	HealthSeverityError HealthSeverity = "error"
)

// HealthCheckRequest selects checks and config context for a health run.
type HealthCheckRequest struct {
	Context        RuntimeContext
	WorkDir        string
	Provider       ProviderID
	Model          ModelID
	Permissions    PermissionMode
	Sandbox        SandboxMode
	Timeout        time.Duration
	Metadata       map[string]string
	Checks         []HealthCheckID
	RequiredChecks []HealthCheckID
	IncludeRefresh bool
}

// HealthReport is an inspectable, secret-safe health summary.
type HealthReport struct {
	Context         RuntimeContext
	EffectiveConfig EffectiveConfig
	Results         []HealthResult
	OverallStatus   HealthStatus
	GeneratedAt     time.Time
	NativeMetadata  map[string]any
}

// HealthResult records one check outcome.
type HealthResult struct {
	Check          HealthCheckID
	Status         HealthStatus
	Severity       HealthSeverity
	UserDetail     string
	DebugDetail    string
	NativeMetadata map[string]any
	Err            *SDKError
	StartedAt      time.Time
	FinishedAt     time.Time
}

// AggregateHealth fills OverallStatus from the report's check results.
func AggregateHealth(report HealthReport) HealthReport {
	report.OverallStatus = OverallHealthStatus(report.Results)
	return report
}

// OverallHealthStatus returns the most severe status present in results.
func OverallHealthStatus(results []HealthResult) HealthStatus {
	status := HealthReady
	for _, result := range results {
		switch result.Status {
		case HealthUnrecoverable:
			return HealthUnrecoverable
		case HealthTransientFail:
			if status != HealthDegraded {
				status = HealthTransientFail
			}
		case HealthDegraded:
			status = HealthDegraded
		case HealthUnknown:
			if status == HealthReady || status == HealthSkipped || status == HealthUnsupported {
				status = HealthUnknown
			}
		case HealthSkipped, HealthUnsupported:
			if status == HealthReady {
				status = result.Status
			}
		}
	}
	return status
}

// RequiredHealthFailure returns a classified failure when a required check has
// an unrecoverable, transient, degraded, unknown, or unsupported result.
func RequiredHealthFailure(report HealthReport, required []HealthCheckID) *SDKError {
	explicitRequired := len(required) > 0
	requiredSet := make(map[HealthCheckID]bool, len(required))
	for _, check := range required {
		requiredSet[check] = true
	}
	if len(requiredSet) == 0 {
		for _, check := range report.Results {
			requiredSet[check.Check] = true
		}
	}
	seen := make(map[HealthCheckID]bool, len(requiredSet))
	for _, result := range report.Results {
		if !requiredSet[result.Check] {
			continue
		}
		seen[result.Check] = true
		switch result.Status {
		case HealthReady, HealthSkipped:
			continue
		case HealthUnknown:
			if result.Err != nil {
				return result.Err
			}
			return NewError(ErrorHealth, "health preflight", "required health check is unknown", nil, WithDebugDetail(string(result.Check)))
		case HealthUnsupported:
			if result.Err != nil {
				return result.Err
			}
			return NewError(ErrorHealth, "health preflight", "required health check is unsupported", nil, WithDebugDetail(string(result.Check)), WithUserActionable(true), WithUnrecoverable(true))
		default:
			if result.Err != nil {
				return result.Err
			}
			return NewError(ErrorHealth, "health preflight", "required health check failed", nil, WithDebugDetail(string(result.Check)))
		}
	}
	if explicitRequired {
		for check := range requiredSet {
			if !seen[check] {
				return NewError(ErrorHealth, "health preflight", "required health check is missing", nil, WithDebugDetail(string(check)), WithUserActionable(true), WithUnrecoverable(true))
			}
		}
	}
	return nil
}

// ErrorForHealthStatus constructs a classified error for a health status.
func ErrorForHealthStatus(check HealthCheckID, status HealthStatus, category ErrorCategory, userDetail, debugDetail string, cause error) *SDKError {
	opts := []ErrorOption{WithDebugDetail(debugDetail)}
	switch status {
	case HealthUnrecoverable, HealthUnsupported:
		opts = append(opts, WithUnrecoverable(true), WithUserActionable(true), WithRetryable(false))
	case HealthTransientFail, HealthDegraded:
		opts = append(opts, WithRetryable(true), WithFallbackable(true))
	case HealthUnknown:
		opts = append(opts, WithFallbackable(true))
	}
	if category == "" {
		category = ErrorHealth
	}
	if debugDetail == "" {
		opts[0] = WithDebugDetail(string(check))
	}
	return NewError(category, "health "+string(check), userDetail, cause, opts...)
}
