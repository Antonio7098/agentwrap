package agentwrap

import "time"

// LifecycleEvent constructs a canonical lifecycle transition event.
func LifecycleEvent(runID RunID, sessionID SessionID, turnID TurnID, ctx RuntimeContext, seq int64, at time.Time, from, to RunStatus, reason string) Event {
	_ = ctx
	return Event{
		ID:        EventID(string(runID) + "-lifecycle-" + itoa64(seq)),
		RunID:     runID,
		SessionID: sessionID,
		Time:      at,
		Type:      "lifecycle.transition",
		Payload: EventPayloadWithKind(EventLifecycle, EventPayload{
			"from":    string(from),
			"to":      string(to),
			"reason":  reason,
			"turn_id": string(turnID),
		}),
	}
}

// SessionEvent constructs a canonical retained-session relationship event.
func SessionEvent(runID RunID, sessionID SessionID, turnID TurnID, ctx RuntimeContext, seq int64, at time.Time, metadata SessionMetadata) Event {
	_ = ctx
	return Event{
		ID:        EventID(string(runID) + "-session-" + itoa64(seq)),
		RunID:     runID,
		SessionID: sessionID,
		Time:      at,
		Type:      "session.relationship",
		Payload: EventPayloadWithKind(EventSession, EventPayload{
			"requested_action": string(metadata.RequestedAction),
			"relationship":     string(metadata.Relationship),
			"requested_id":     string(metadata.RequestedID),
			"session_id":       string(metadata.ID),
			"unsupported":      metadata.UnsupportedReason,
			"best_effort":      metadata.BestEffort,
			"turn_id":          string(turnID),
		}),
	}
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
