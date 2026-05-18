package agentwrap

// RunID identifies one runtime execution attempt.
type RunID string

// SessionID identifies retained runtime context when a runtime supports it.
type SessionID string

// TurnID correlates one caller prompt or follow-up inside a run/session.
type TurnID string

// EventID identifies a canonical event within a run.
type EventID string

// RuntimeKind identifies a runtime implementation. Values are intentionally
// open so additional runtimes do not require SDK API changes.
type RuntimeKind string

// ProviderID identifies the configured provider or provider instance.
type ProviderID string

// ModelID identifies the configured model.
type ModelID string

// ArtifactID identifies an artifact reference emitted by a runtime.
type ArtifactID string
