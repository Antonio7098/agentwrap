package agentwrap

import "time"

// LifecycleEvent constructs a canonical lifecycle transition event.
func LifecycleEvent(runID RunID, sessionID SessionID, turnID TurnID, ctx RuntimeContext, seq int64, at time.Time, from, to LifecycleState, reason string) Event {
	return Event{
		ID:        EventID(string(runID) + "-lifecycle-" + itoa64(seq)),
		Sequence:  seq,
		RunID:     runID,
		SessionID: sessionID,
		TurnID:    turnID,
		Context:   ctx,
		Time:      at,
		Category:  EventLifecycle,
		Type:      "lifecycle.transition",
		Payload: EventPayload{
			"from":   string(from),
			"to":     string(to),
			"reason": reason,
		},
	}
}

// SessionEvent constructs a canonical retained-session relationship event.
func SessionEvent(runID RunID, sessionID SessionID, turnID TurnID, ctx RuntimeContext, seq int64, at time.Time, metadata SessionMetadata) Event {
	return Event{
		ID:        EventID(string(runID) + "-session-" + itoa64(seq)),
		Sequence:  seq,
		RunID:     runID,
		SessionID: sessionID,
		TurnID:    turnID,
		Context:   ctx,
		Time:      at,
		Category:  EventSession,
		Type:      "session.relationship",
		Payload: EventPayload{
			"requested_action": string(metadata.RequestedAction),
			"relationship":     string(metadata.Relationship),
			"requested_id":     string(metadata.RequestedID),
			"session_id":       string(metadata.ID),
			"unsupported":      metadata.UnsupportedReason,
			"best_effort":      metadata.BestEffort,
		},
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
