# agentwrap Documentation

`agentwrap` is a Go SDK for supervising agentic coding runtimes from product workflows. The SDK keeps product code insulated from runtime-specific process handling, event schemas, and failure formats by exposing a runtime-neutral contract at the module root and adapter packages for concrete runtimes.

This documentation is the canonical guide for the current repository state.

## Contents

- [Architecture](architecture.md): package boundaries, data flow, and design invariants.
- [Core API](core-api.md): `Runtime`, `Run`, `RunRequest`, `RunResult`, capabilities, sessions, and artifacts.
- [Events And Metadata](events-and-metadata.md): canonical events, raw payload safety, run metadata, usage, and cleanup.
- [Errors, Config, And Health](errors-config-health.md): classified errors, effective config, and preflight checks.
- [Wrappers](wrappers.md): resilience policy execution, validation and repair, observability, sinks, and stores.
- [OpenCode Adapter](opencode-adapter.md): CLI invocation, structured output projection, permissions, sessions, health, and limits.
- [Integration Guide](integration-guide.md): recommended composition patterns and examples.
- [Development](development.md): test strategy, fixtures, and repository guardrails.

## Current Surface

The repository currently provides:

- A runtime-neutral root package, `github.com/Antonio7098/agentwrap`.
- Public contracts for starting runs, consuming events, waiting for results, cancellation, capabilities, classified errors, health checks, permission policy summaries, validation, repair, policy execution, observability, and run inspection.
- An `opencode` adapter that launches `opencode run --format json`, decodes JSON-lines output, projects native records into canonical events, and reports classified results.
- Runtime-neutral wrappers that can be composed around any adapter:
  - `PolicyRunner`
  - `ValidatingRuntime`
  - `ObservingRuntime`
- A deterministic in-memory `RunStore` reference implementation.
- Private `internal/testkit` fakes and structured fixtures for contract tests.

## Design Position

`agentwrap` is an SDK layer, not a workflow product. It does not implement product-specific planning flows, durable database selection, global provider throttling, or runtime server transports. Those concerns are intentionally caller-owned or deferred until concrete adapters need them.

The root package owns the stable contract. Adapter packages own native runtime details.

