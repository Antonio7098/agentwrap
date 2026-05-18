package agentwrap

import (
	"errors"
	"testing"
)

func TestSDKErrorClassificationAndWrapping(t *testing.T) {
	cause := errors.New("native failure")
	err := NewError(ErrorRateLimit, "runtime.start", "provider asked caller to retry later", cause)
	err.Retryable = true
	err.Fallbackable = true

	if !errors.Is(err, cause) {
		t.Fatal("SDKError does not unwrap cause")
	}
	var got *SDKError
	if !ErrorAs(err, &got) {
		t.Fatal("ErrorAs did not find SDKError")
	}
	if got.Category != ErrorRateLimit {
		t.Fatalf("Category = %q, want %q", got.Category, ErrorRateLimit)
	}
	if !got.Retryable || !got.Fallbackable {
		t.Fatalf("classification flags = retryable:%v fallbackable:%v", got.Retryable, got.Fallbackable)
	}
	if got.SafeDetail == "" {
		t.Fatal("SafeDetail is empty")
	}
}
