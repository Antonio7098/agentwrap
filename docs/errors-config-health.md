# Errors, Config, And Health

## Classified Errors

All public SDK failures should be classified as `SDKError`.

Current categories:

- `configuration`
- `health`
- `runtime_unavailable`
- `provider_unavailable`
- `model_unavailable`
- `authentication`
- `permission`
- `rate_limit`
- `timeout`
- `cancellation`
- `malformed_event`
- `runtime_exit`
- `validation`
- `repair_exhausted`
- `cleanup`
- `unknown`

Use `errors.As` or `agentwrap.ErrorAs` instead of string matching.

## SDKError Fields

`SDKError` separates user-safe and diagnostic fields:

- `Category`: stable machine-readable category.
- `Operation`: operation where the error occurred.
- `UserDetail`: safe user-facing summary.
- `DebugDetail`: diagnostic detail.
- `StatusCode`, `ResponseHeaders`, `ResponseBody`: HTTP-like provider facts when available.
- `Provider`, `Model`, `RuntimeKind`: runtime context.
- `ExitCode`, `Signal`, `NativeType`: process/native details.
- `RetryAfter`: retry hint.
- `Metadata`: redacted structured detail.
- `Cause`: wrapped underlying error.

Helpers such as `WithResponse` and `WithMetadata` redact sensitive values.

## Effective Config

`EffectiveConfig` is a source-aware merged configuration view. It records values and provenance for:

- runtime kind and name
- executable
- provider and model
- workdir
- permissions
- sandbox
- timeout
- session ID
- metadata
- secret presence

Config source values include:

- `default`
- `adapter_option`
- `environment`
- `config_provider`
- `caller_request`
- `runtime_discovered`

`MergeEffectiveConfig` applies layers from low to high precedence. `CallerConfigLayer` converts `RunRequest` values into a high-precedence layer. `ValidateEffectiveConfig` rejects invalid values such as empty configured executable/provider/model/workdir or negative timeout.

## Health Checks

Adapters that can run cheap preflight checks implement:

```go
type HealthChecker interface {
    CheckHealth(context.Context, HealthCheckRequest) (HealthReport, error)
}
```

Health checks do not start billable agent work.

Current check IDs:

- `runtime_available`
- `structured_output`
- `workdir`
- `config`
- `provider`
- `model`
- `authentication`
- `runtime_paths`

Health statuses:

- `ready`
- `degraded`
- `transient_failure`
- `unrecoverable_failure`
- `unknown`
- `skipped`
- `unsupported`

`AggregateHealth` computes the report-level status. `RequiredHealthFailure` converts required check failures into a classified `SDKError`.

## Required Health

`RunRequest.RequireHealth` asks an adapter to block launch unless those preflight checks are acceptable. The OpenCode adapter runs required checks before process start and returns `ErrorHealth` or a more specific classified error when required readiness cannot be proven.

