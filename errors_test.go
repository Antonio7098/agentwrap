package agentwrap

import (
	"errors"
	"testing"
)

func TestSDKErrorClassificationAndWrapping(t *testing.T) {
	cause := errors.New("native failure")
	err := NewError(
		ErrorRateLimit,
		"runtime.start",
		"provider asked caller to retry later",
		cause,
		WithDebugDetail("http 429 from provider"),
		WithFallbackable(true),
	)

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
	if got.UserDetail == "" || got.SafeDetail == "" {
		t.Fatalf("user-safe detail is empty: UserDetail=%q SafeDetail=%q", got.UserDetail, got.SafeDetail)
	}
	if got.DebugDetail != "http 429 from provider" {
		t.Fatalf("DebugDetail = %q", got.DebugDetail)
	}
}

func TestNewErrorDefaultClassification(t *testing.T) {
	for _, tc := range []struct {
		category       ErrorCategory
		retryable      bool
		fallbackable   bool
		userActionable bool
	}{
		{category: ErrorConfiguration, userActionable: true},
		{category: ErrorRuntimeUnavailable, retryable: true, fallbackable: true},
		{category: ErrorRateLimit, retryable: true},
		{category: ErrorMalformedEvent, fallbackable: true},
	} {
		t.Run(string(tc.category), func(t *testing.T) {
			err := NewError(tc.category, "test", "detail", nil)
			if err.Retryable != tc.retryable {
				t.Fatalf("Retryable = %v, want %v", err.Retryable, tc.retryable)
			}
			if err.Fallbackable != tc.fallbackable {
				t.Fatalf("Fallbackable = %v, want %v", err.Fallbackable, tc.fallbackable)
			}
			if err.UserActionable != tc.userActionable {
				t.Fatalf("UserActionable = %v, want %v", err.UserActionable, tc.userActionable)
			}
		})
	}
}
