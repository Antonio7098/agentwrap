package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type nativeRecord struct {
	Type      string         `json:"type"`
	Timestamp any            `json:"timestamp,omitempty"`
	SessionID string         `json:"sessionID,omitempty"`
	Data      map[string]any `json:"-"`
	Raw       []byte         `json:"-"`
	Line      int64          `json:"-"`
}

type decodeError struct {
	line int64
	raw  []byte
	err  error
}

func (e *decodeError) Error() string {
	return fmt.Sprintf("decode opencode event line %d: %v", e.line, e.err)
}

func decodeNativeLine(line []byte, pos int64) (nativeRecord, error) {
	raw := append([]byte(nil), bytes.TrimRight(line, "\r\n")...)
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nativeRecord{}, &decodeError{line: pos, raw: raw, err: err}
	}
	t, _ := data["type"].(string)
	if t == "" {
		return nativeRecord{}, &decodeError{line: pos, raw: raw, err: fmt.Errorf("missing string field type")}
	}
	sessionID, _ := data["sessionID"].(string)
	return nativeRecord{
		Type:      t,
		Timestamp: data["timestamp"],
		SessionID: sessionID,
		Data:      data,
		Raw:       raw,
		Line:      pos,
	}, nil
}

func scanNativeRecords(ctx context.Context, r io.Reader, emit func(nativeRecord) error) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256*1024), 16*1024*1024)
	var pos int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !scanner.Scan() {
			break
		}
		pos++
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			return &decodeError{line: pos, raw: append([]byte(nil), line...), err: fmt.Errorf("blank structured output record")}
		}
		record, err := decodeNativeLine(line, pos)
		if err != nil {
			return err
		}
		if err := emit(record); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return &decodeError{line: pos + 1, err: err}
		}
		return &decodeError{line: pos + 1, err: err}
	}
	return nil
}
