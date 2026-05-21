# Integration Guide

## Minimal Run

```go
package main

import (
    "context"
    "fmt"

    "github.com/Antonio7098/agentwrap"
    "github.com/Antonio7098/agentwrap/opencode"
)

func main() {
    ctx := context.Background()
    runtime := opencode.NewRuntime()

    run, err := runtime.StartRun(ctx, agentwrap.RunRequest{
        Prompt:  "Inspect this repository and summarize the public API.",
        WorkDir: ".",
    })
    if err != nil {
        panic(err)
    }

    go func() {
        for event := range run.Events() {
            fmt.Println(event.Kind(), event.Type)
        }
    }()

    result, err := run.Wait(ctx)
    if err != nil {
        fmt.Println(result.Status, err)
        return
    }
    fmt.Println(result.Status)
}
```

## Health-Gated Run

Use `RequireHealth` when launch should be blocked unless preflight checks pass:

```go
req := agentwrap.RunRequest{
    Prompt:   "Make the requested change.",
    WorkDir:  repoPath,
    Provider: "provider-id",
    Model:    "model-id",
    RequireHealth: []agentwrap.HealthCheckID{
        agentwrap.HealthCheckRuntimeAvailable,
        agentwrap.HealthCheckStructuredOutput,
        agentwrap.HealthCheckWorkDir,
        agentwrap.HealthCheckProvider,
        agentwrap.HealthCheckModel,
    },
}
```

For manual health inspection:

```go
checker := opencode.NewRuntime()
report, err := checker.CheckHealth(ctx, agentwrap.HealthCheckRequest{
    WorkDir:  repoPath,
    Provider: "provider-id",
    Model:    "model-id",
})
```

## Permission Policy

```go
policy := &agentwrap.PermissionPolicy{
    Default: agentwrap.PermissionActionAsk,
    Tools: map[agentwrap.PermissionTool]agentwrap.PermissionAction{
        agentwrap.PermissionToolRead:  agentwrap.PermissionActionAllow,
        agentwrap.PermissionToolEdit:  agentwrap.PermissionActionAllow,
        agentwrap.PermissionToolShell: agentwrap.PermissionActionAsk,
    },
}

req := agentwrap.RunRequest{
    Prompt:           "Update documentation.",
    WorkDir:          repoPath,
    PermissionPolicy: policy,
}
```

For path-level rules with the current OpenCode subprocess adapter, choose whether unsupported rules should fail fast or be recorded as best-effort:

```go
policy.UnsupportedBehavior = agentwrap.PermissionUnsupportedBestEffort
```

## Retry And Fallback

```go
primary := opencode.NewRuntime()
fallback := opencode.NewRuntime()

runtime := agentwrap.PolicyRunner{
    Runtime: primary,
    Policy: agentwrap.BasicPolicy{
        MaxAttemptsPerTarget: 2,
        RetryRateLimits:      true,
        Backoff: agentwrap.ExponentialBackoff{
            Initial: 500 * time.Millisecond,
            Factor:  2,
            Max:     5 * time.Second,
        },
        Fallbacks: []agentwrap.FallbackAlternative{
            {
                Name:    "secondary-model",
                Runtime: fallback,
                Request: agentwrap.RunRequest{
                    Provider: "fallback-provider-id",
                    Model:    "fallback-model-id",
                },
            },
        },
    },
}
```

The fallback request is an override target. Preserve fields such as workdir, prompt, timeout, permissions, and validation when your policy needs them.

## Validation

```go
runtime := agentwrap.ValidatingRuntime{
    Runtime: opencode.NewRuntime(),
    Spec: agentwrap.ValidationSpec{
        Expectations: []agentwrap.ValidationExpectation{
            {
                ID:       "readme-exists",
                Kind:     agentwrap.ExpectationFile,
                Severity: agentwrap.ExpectationRequired,
                Path:     "README.md",
            },
        },
        Repair: agentwrap.RepairConfig{
            MaxAttempts: 1,
        },
    },
}
```

Use custom validators for product-specific rules:

```go
validator := agentwrap.ValidatorFunc(func(ctx context.Context, vctx agentwrap.ValidationContext) agentwrap.ValidationCheck {
    return agentwrap.ValidationCheck{
        ExpectationID: "custom-rule",
        Kind:          agentwrap.ExpectationCustom,
        Severity:      agentwrap.ExpectationRequired,
        Passed:        true,
    }
})
```

## Observability

```go
store := agentwrap.NewMemoryRunStore()

runtime := agentwrap.ObservingRuntime{
    Runtime: opencode.NewRuntime(),
    Store:   store,
}

run, err := runtime.StartRun(ctx, req)
```

After completion:

```go
record, ok, err := runtime.GetCompletedRun(ctx, run.ID())
events, err := runtime.ListRunEvents(ctx, run.ID())
```

For production persistence, implement `RunStore` against your database or queue. Keep raw payload handling aligned with your data retention and privacy policy.

## Error Handling

```go
result, err := run.Wait(ctx)
if err != nil {
    var sdkErr *agentwrap.SDKError
    if agentwrap.ErrorAs(err, &sdkErr) {
        switch sdkErr.Category {
        case agentwrap.ErrorRateLimit:
            // honor sdkErr.RetryAfter when set
        case agentwrap.ErrorPermission:
            // ask user or fail workflow
        }
    }
    _ = result
}
```

Do not parse error strings. Use `SDKError.Category` and structured fields.
