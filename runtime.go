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

// ModelsRequest scopes a model enumeration request.
type ModelsRequest struct {
	// Provider optionally filters models to one provider.
	Provider ProviderID
	// WorkDir is the optional working directory for the listing command.
	WorkDir string
}

// ModelInfo describes one model available to the runtime.
type ModelInfo struct {
	Provider ProviderID
	ID       ModelID
	Name     string
}

// ModelLister is an optional runtime capability for enumerating available
// models. Callers should type-assert for it; middleware wrappers that embed a
// ModelLister forward the request.
type ModelLister interface {
	ListModels(context.Context, ModelsRequest) ([]ModelInfo, error)
}

// RunRequest contains the minimal caller input needed by the runtime contract.
type RunRequest struct {
	Prompt           string
	WorkDir          string
	SessionID        SessionID
	TurnID           TurnID
	Provider         ProviderID
	Model            ModelID
	Permissions      PermissionMode
	PermissionPolicy *PermissionPolicy
	Sandbox          SandboxMode
	Timeout          time.Duration
	Metadata         map[string]string
	WantSession      bool
	SessionAction    SessionAction
	RequireCaps      []Capability
	RequireHealth    []HealthCheckID
	Validation       *ValidationSpec
	PromptCache      PromptCacheDirective
}

// PermissionMode is an open placeholder for future permission policies.
type PermissionMode string

// SandboxMode is an open placeholder for future sandbox requirements.
type SandboxMode string

// RunResult is the final caller-visible result for a run.
type RunResult struct {
	RunID     RunID
	SessionID SessionID
	TurnID    TurnID
	Status    RunStatus
	// TerminalOutput is the last bounded assistant output observed by the
	// adapter. It lets caller-defined validators inspect structured results
	// without depending on runtime-native event payloads.
	TerminalOutput string
	Metadata       RunMetadata
	Artifacts      []ArtifactRef
	Warnings       []string
	Usage          Usage
	StartedAt      time.Time
	FinishedAt     time.Time
	Err            *SDKError
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
	// CapabilityPromptCacheAdvisory means the adapter accepts, validates, and
	// audits cache directives while leaving placement to the downstream runtime.
	CapabilityPromptCacheAdvisory Capability = "prompt_cache_advisory"
	// CapabilityPromptCacheNative means the adapter can apply the caller's
	// routing key and exact byte breakpoint at the provider boundary.
	CapabilityPromptCacheNative Capability = "prompt_cache_native"
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
