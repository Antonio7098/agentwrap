// Package agentwrap defines the public SDK contract for supervising agentic
// coding runtimes.
//
// The contract is runtime-neutral: callers start runs, consume canonical
// events, inspect metadata, and classify failures without depending on any
// runtime's native process or event schema.
//
// HealthChecker is an optional SDK surface for adapters that can run cheap
// preflight checks independently from StartRun. Health reports and effective
// configuration summaries are source-aware and redact secret values by default.
//
// PolicyRunner wraps runtimes with explicit resilience policies for bounded
// retry, fallback, backoff, and rate-limit handling. Runtime adapters still
// report their native attempt outcomes directly; policy execution records every
// attempt and emits canonical retry, fallback, and rate-limit events.
//
// RunResult.Status preserves the primary run outcome. Cleanup of owned runtime
// resources is reported separately through RunMetadata.Cleanup and lifecycle
// events so a successful or failed run can still expose cleanup diagnostics.
package agentwrap
