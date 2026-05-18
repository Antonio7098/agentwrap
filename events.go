package agentwrap

import "time"

// Event is the canonical event envelope emitted by runtimes.
type Event struct {
	ID            EventID
	Sequence      int64
	RunID         RunID
	SessionID     SessionID
	TurnID        TurnID
	CorrelationID CorrelationID
	CauseEventID  EventID
	Context       RuntimeContext
	Time          time.Time
	Category      EventCategory
	Type          string
	Payload       EventPayload
	Raw           *RawPayload
}

// EventPayload is intentionally open for future event types while core
// categories still use canonical envelope fields.
type EventPayload map[string]any

// EventCategory groups events into caller-facing behavior.
type EventCategory string

const (
	EventLifecycle        EventCategory = "lifecycle"
	EventSession          EventCategory = "session"
	EventMessage          EventCategory = "message"
	EventProgress         EventCategory = "progress"
	EventTool             EventCategory = "tool"
	EventArtifact         EventCategory = "artifact"
	EventPermission       EventCategory = "permission"
	EventBlocking         EventCategory = "blocking"
	EventUsage            EventCategory = "usage"
	EventWarning          EventCategory = "warning"
	EventRecoverableError EventCategory = "recoverable_error"
	EventFatalError       EventCategory = "fatal_error"
	EventRateLimit        EventCategory = "rate_limit"
	EventValidation       EventCategory = "validation"
	EventRetry            EventCategory = "retry"
	EventFallback         EventCategory = "fallback"
	EventFinalResult      EventCategory = "final_result"
	EventUnknown          EventCategory = "unknown"
	EventNativeExtension  EventCategory = "native_extension"
)

// RawPayload preserves native structured runtime data for diagnostics. Callers
// must treat it as sensitive until adapter-specific redaction rules say it is
// safe to persist or display.
type RawPayload struct {
	Source   string
	Encoding string
	Data     []byte
	Safe     bool
}
