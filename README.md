# agentwrap

`agentwrap` is a Go SDK skeleton for supervising agentic coding runtimes from product workflows.

Sprint 1 establishes only the project boundary and test harness:

- public SDK package at the module root
- private structured-event fixtures and fake lifecycle tests under `internal/testkit`

The public runtime, session, event, lifecycle, and error contracts are intentionally deferred to later sprints.

## Development

Requires Go 1.22 or newer.

```sh
go test ./...
```

Format code before committing:

```sh
gofmt -w .
```

## Scope Guardrails

- No real OpenCode invocation in Sprint 1.
- No public runtime contract until Sprint 2.
- No UltraPlan workflow logic in this SDK.
- CLI-oriented study material is treated as internal engineering evidence only, not product direction.
