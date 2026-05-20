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
// PermissionPolicy lets callers choose the permission posture when a run is
// initialized. Adapters translate supported SDK permission tools to native
// runtime configuration, classify unsupported features before launch, and record
// safe permission audit metadata in RunMetadata.Permissions.
//
// ValidatingRuntime wraps any Runtime with output validation and bounded repair.
// Built-in expectations check durable files, directories, artifact references,
// Markdown artifacts against template files, JSON well-formedness and minimal
// required fields, metadata fields, and caller-defined validators. Validation
// and repair facts are emitted as canonical validation events and stored in
// RunMetadata.Validation and RunMetadata.Repair.
//
// ObservingRuntime wraps any Runtime with active/completed run records, ordered
// event records, optional EventSink fan-out, and optional RunStore persistence.
// The in-memory store is a deterministic reference implementation; production
// durability remains caller-owned. Unsafe raw native payload bytes are omitted
// from persisted event records by default and recorded with omission metadata.
//
// RunResult.Status preserves the primary run outcome. Cleanup of owned runtime
// resources is reported separately through RunMetadata.Cleanup and lifecycle
// events so a successful or failed run can still expose cleanup diagnostics.
package agentwrap
