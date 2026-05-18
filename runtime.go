package agentwrap

import (
	"context"
	"time"
)

// Runtime starts runtime work and reports what contract features it supports.
type Runtime interface {
	StartRun(context.Context, RunRequest) (Run, error)
	Capabilities(context.Context) (Capabilities, error)
}

// Run is the handle returned for a started runtime run.
type Run interface {
	ID() RunID
	Events() <-chan Event
	Wait(context.Context) (RunResult, error)
	Cancel(context.Context) error
}

// RunRequest contains the minimal caller input needed by the runtime contract.
type RunRequest struct {
	Prompt        string
	WorkDir       string
	SessionID     SessionID
	TurnID        TurnID
	Provider      ProviderID
	Model         ModelID
	Permissions   PermissionMode
	Sandbox       SandboxMode
	Timeout       time.Duration
	Metadata      map[string]string
	WantSession   bool
	SessionAction SessionAction
	RequireCaps   []Capability
}

// PermissionMode is an open placeholder for future permission policies.
type PermissionMode string

// SandboxMode is an open placeholder for future sandbox requirements.
type SandboxMode string

// RunResult is the final caller-visible result for a run.
type RunResult struct {
	RunID      RunID
	SessionID  SessionID
	TurnID     TurnID
	Status     LifecycleState
	Metadata   RunMetadata
	Artifacts  []ArtifactRef
	Warnings   []string
	Usage      Usage
	StartedAt  time.Time
	FinishedAt time.Time
	Err        *SDKError
}

// Capabilities describes runtime-supported contract features.
type Capabilities struct {
	RuntimeKind RuntimeKind
	Features    map[Capability]CapabilitySupport
	Unsupported []UnsupportedCapability
}

// Supports reports whether a capability is explicitly supported.
func (c Capabilities) Supports(capability Capability) bool {
	return c.Features[capability].Supported
}

// Capability is an open contract-level runtime feature identifier.
type Capability string

const (
	CapabilitySessions         Capability = "sessions"
	CapabilitySessionContinue  Capability = "session_continue"
	CapabilitySessionFork      Capability = "session_fork"
	CapabilitySessionReplace   Capability = "session_replace"
	CapabilitySessionRelease   Capability = "session_release"
	CapabilityCancellation     Capability = "cancellation"
	CapabilityStructuredEvents Capability = "structured_events"
	CapabilityRawPayloads      Capability = "raw_payloads"
	CapabilityArtifacts        Capability = "artifacts"
	CapabilityPermissions      Capability = "permissions"
	CapabilityUsage            Capability = "usage"
	CapabilityValidationEvents Capability = "validation_events"
)

// CapabilitySupport records support status and safe explanation text.
type CapabilitySupport struct {
	Supported bool
	Detail    string
}

// UnsupportedCapability records an unsupported feature request.
type UnsupportedCapability struct {
	Capability Capability
	Reason     string
}
