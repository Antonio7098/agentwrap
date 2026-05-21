# Events And Metadata

## Canonical Event Envelope

Every runtime event uses:

```go
type Event struct {
    ID        EventID
    RunID     RunID
    SessionID SessionID
    Time      time.Time
    Type      string
    Payload   EventPayload
    Raw       *RawPayload
}
```

`Type` preserves the native or adapter-defined event name. The SDK event projection is stored in `Payload["event_kind"]` and is available through `Event.Kind()`.

## Event Kinds

Current canonical projections are:

- `lifecycle`
- `session`
- `message`
- `progress`
- `tool`
- `artifact`
- `permission`
- `blocking`
- `usage`
- `warning`
- `fatal_error`
- `rate_limit`
- `validation`
- `retry`
- `fallback`
- `final_result`
- `native_extension`

Adapters may emit native event types that do not map to a specific SDK category. Those should be projected as `native_extension`.

## Raw Payloads

`RawPayload` preserves native runtime data for diagnostics:

- `Source`
- `Encoding`
- `Data`
- `Safe`

Raw payloads must be treated as sensitive unless `Safe` is true. `ObservingRuntime` persists safe raw bytes by default and omits unsafe raw bytes unless `PersistencePolicy.PersistUnsafeRawPayloads` is enabled. Omitted payloads still record presence, source, encoding, safety, and omission reason.

## Lifecycle Events

`LifecycleEvent` constructs canonical lifecycle transition events with:

- `from`
- `to`
- `reason`
- `turn_id`

Adapters and wrappers use lifecycle events to expose phase transitions without changing the final result contract.

## Session Events

`SessionEvent` reports retained-session relationship facts:

- requested action
- relationship
- requested session ID
- resolved session ID
- unsupported reason
- best-effort flag
- turn ID

## RunMetadata

`RunMetadata` is the audit and dashboard record attached to final results. It includes:

- Runtime context.
- Parent run ID.
- Attempt information.
- Policy decisions.
- Status and timing.
- Session metadata.
- Permission metadata.
- Cleanup metadata.
- Validation and repair metadata.
- Artifact references.
- Warnings and errors.
- Usage, estimated cost, and throughput.
- Native metadata.

Metadata is best-effort unless a specific wrapper or adapter documents a stronger guarantee.

## Usage

`Usage` stores token counts as pointers:

- `InputTokens`
- `OutputTokens`
- `TotalTokens`

Unknown values remain `nil`. Code should not treat unknown usage as zero.

## Cleanup

`CleanupMetadata` reports owned-resource cleanup:

- `Attempted`
- `Completed`
- `Failed`
- `Error`

Cleanup is separate from the primary run result. A completed run can still include cleanup diagnostics.

