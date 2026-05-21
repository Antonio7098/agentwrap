# Development

## Requirements

The module targets Go 1.22 or newer.

```sh
go test ./...
```

Format before committing:

```sh
gofmt -w .
```

## Repository Layout

```text
.
├── *.go                  root agentwrap SDK contract and wrappers
├── *_test.go             root package tests
├── opencode/             OpenCode CLI adapter
├── internal/testkit/     private test fakes and fixtures
├── README.md             concise project overview
└── docs/                 canonical documentation
```

## Test Strategy

The repository uses focused unit and contract tests:

- Root package tests cover config, errors, health, lifecycle events, permissions, policies, validation, redaction, and observability.
- `opencode` tests cover process behavior, structured decoding, projection, health, permissions, and integration behavior through fake process runners and fixtures.
- `internal/testkit` tests prove fake runtime and fixture behavior used by other tests.

OpenCode real invocation remains opt-in through the gated smoke test. Normal `go test ./...` should not require billable runtime work.

## Adding A Runtime Adapter

A new adapter should:

1. Implement `agentwrap.Runtime`.
2. Keep native process and event schemas inside the adapter package.
3. Project native events into canonical `Event` values.
4. Preserve native raw payloads only as `RawPayload`, marking safety accurately.
5. Classify failures into `SDKError`.
6. Report `Capabilities`.
7. Implement `HealthChecker` when cheap preflight checks are available.
8. Translate `PermissionPolicy` before launch or fail explicitly when unsupported features cannot be enforced.
9. Keep cleanup metadata separate from the primary result status.

## Adding A Wrapper

A wrapper should:

1. Implement `agentwrap.Runtime`.
2. Accept another `Runtime`.
3. Avoid depending on adapter-specific details.
4. Forward or synthesize canonical events.
5. Preserve classified errors.
6. Store safe, bounded metadata.
7. Document whether it changes logical success semantics.

## Guardrails

- Do not move retry, validation, or observability logic into adapters when it can stay runtime-neutral.
- Do not persist unsafe raw payload bytes by default.
- Do not coerce unknown usage values to zero.
- Do not silently ignore unsupported permission policy features.
- Do not make health checks start agent work.
- Do not add product-specific workflow logic to the SDK root.

