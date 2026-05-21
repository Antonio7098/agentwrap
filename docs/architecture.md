# Architecture

`agentwrap` separates runtime supervision into three layers:

1. The root SDK contract in package `agentwrap`.
2. Runtime-neutral wrappers that add cross-cutting behavior around any `Runtime`.
3. Adapter packages such as `opencode` that translate native runtime behavior into the SDK contract.

## Package Boundaries

### Root Package

The root package defines the public SDK vocabulary:

- Runtime lifecycle: `Runtime`, `Run`, `RunRequest`, `RunResult`, `RunStatus`.
- Eventing: `Event`, `EventPayload`, `EventKind`, `RawPayload`.
- Metadata: `RunMetadata`, `AttemptSummary`, `SessionMetadata`, `PermissionMetadata`, `ValidationMetadata`, `RepairMetadata`, `CleanupMetadata`.
- Errors: `SDKError`, `ErrorCategory`, and error construction options.
- Capabilities: `Capability`, `Capabilities`, `UnsupportedCapability`.
- Health: `HealthChecker`, `HealthCheckRequest`, `HealthReport`, `HealthResult`.
- Config: `EffectiveConfig`, `ConfigLayer`, and source-aware merge/validation helpers.
- Wrappers: `PolicyRunner`, `ValidatingRuntime`, `ObservingRuntime`.
- Persistence interfaces: `EventSink`, `RunStore`, `RunInspector`, `MemoryRunStore`.

No root package type should depend on a native runtime schema.

### `opencode` Package

The `opencode` package adapts the OpenCode CLI:

- Starts `opencode run --format json`.
- Passes working directory, provider/model, session, permissions, environment, extra args, and prompt.
- Decodes native JSON-lines stdout.
- Projects native records into canonical `agentwrap.Event` values.
- Preserves raw native records as unsafe diagnostic payloads by default.
- Classifies process, decode, timeout, cancellation, rate-limit, permission, and health failures into `SDKError`.
- Implements `HealthChecker`.

Native OpenCode details stay inside this package.

### `internal/testkit`

`internal/testkit` is private repository support for tests. It contains fake runtimes, fake lifecycle implementations, and event fixtures. It is not a public SDK surface.

## Runtime Flow

A caller starts work with:

```go
run, err := runtime.StartRun(ctx, request)
```

If start succeeds, the caller receives a `Run` handle:

- `ID()` returns the logical run handle ID.
- `Events()` streams canonical events until the run ends.
- `Wait(ctx)` returns the final `RunResult` and any terminal error.
- `Cancel(ctx)` requests best-effort cancellation.

Adapters are responsible for translating native execution into this contract. Wrappers may sit between caller and adapter to add policy, validation, or observability without adapter changes.

## Recommended Composition

A typical production composition is:

```text
caller
  -> ObservingRuntime
  -> ValidatingRuntime
  -> PolicyRunner
  -> opencode.Runtime
```

The order is caller-owned. Common tradeoffs:

- Put `ObservingRuntime` outside everything to observe logical wrapper events and final merged metadata.
- Put `PolicyRunner` close to the adapter when retries and fallback should wrap raw runtime attempts.
- Put `ValidatingRuntime` outside policy when validation failure should be the final logical result.
- Put `ValidatingRuntime` inside policy only when validation failures should be eligible for policy fallback.

## Invariants

- The root package remains runtime-neutral.
- `RunResult.Status` preserves the primary run outcome.
- Cleanup diagnostics are reported in `RunMetadata.Cleanup`; cleanup does not silently overwrite a successful primary run unless cleanup itself becomes the only terminal error.
- Unknown usage values remain `nil`, not zero.
- Unsafe raw native payload bytes are not persisted by default.
- Retry, fallback, validation repair, and health blocking are explicit and bounded.
- Permission policies are translated or rejected before runtime launch when unsupported features cannot be enforced.

