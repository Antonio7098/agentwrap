package agentwrap

import "time"

// RuntimeContext identifies the runtime/provider/model context for events,
// artifacts, and final results.
type RuntimeContext struct {
	RuntimeKind RuntimeKind
	RuntimeName string
	Provider    ProviderID
	Model       ModelID
}

// RunMetadata contains dashboard- and audit-oriented run metadata. Most fields
// are best-effort until later adapter and observability sprints define stronger
// guarantees.
type RunMetadata struct {
	Context        RuntimeContext
	ParentRunID    RunID
	Attempt        int
	Status         LifecycleState
	StartedAt      time.Time
	FinishedAt     time.Time
	Duration       time.Duration
	Session        SessionMetadata
	Cleanup        CleanupMetadata
	Artifacts      []ArtifactRef
	Warnings       []string
	Errors         []SDKError
	Usage          Usage
	EstimatedCost  *CostEstimate
	ThroughputTPS  *float64
	NativeMetadata map[string]any
}

// CleanupMetadata reports owned-resource cleanup separately from the primary
// run outcome.
type CleanupMetadata struct {
	Attempted bool
	Completed bool
	Failed    bool
	Error     *SDKError
}

// SessionAction identifies the retained-session behavior requested by a caller.
type SessionAction string

const (
	SessionActionDefault  SessionAction = ""
	SessionActionFresh    SessionAction = "fresh"
	SessionActionContinue SessionAction = "continue"
	SessionActionFork     SessionAction = "fork"
	SessionActionReplace  SessionAction = "replace"
	SessionActionRelease  SessionAction = "release"
)

// SessionRelationship identifies the runtime-resolved relationship between the
// request and resulting session.
type SessionRelationship string

const (
	SessionRelationshipNone        SessionRelationship = ""
	SessionRelationshipFresh       SessionRelationship = "fresh"
	SessionRelationshipSame        SessionRelationship = "same"
	SessionRelationshipForked      SessionRelationship = "forked"
	SessionRelationshipReplaced    SessionRelationship = "replaced"
	SessionRelationshipReleased    SessionRelationship = "released"
	SessionRelationshipUnsupported SessionRelationship = "unsupported"
	SessionRelationshipBestEffort  SessionRelationship = "best_effort"
)

// SessionMetadata describes retained-session state when supported.
type SessionMetadata struct {
	ID                SessionID
	RequestedID       SessionID
	RequestedAction   SessionAction
	Relationship      SessionRelationship
	Retained          bool
	Continued         bool
	ForkedFrom        SessionID
	Replaced          SessionID
	Unsupported       []UnsupportedCapability
	UnsupportedReason string
	BestEffort        bool
}

// ArtifactRef references durable runtime output without embedding large data.
type ArtifactRef struct {
	ID          ArtifactID
	URI         string
	Kind        string
	Description string
	Metadata    map[string]string
}

// Usage records best-effort usage values. Unknown values remain nil.
type Usage struct {
	InputTokens  *int64
	OutputTokens *int64
	TotalTokens  *int64
	Native       map[string]any
}

// CostEstimate records best-effort cost metadata.
type CostEstimate struct {
	Amount   float64
	Currency string
	Estimate bool
}
