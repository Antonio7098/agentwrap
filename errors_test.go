package agentwrap

import (
	"errors"
	"testing"
	"time"
)

func TestSDKErrorFactsAndWrapping(t *testing.T) {
	cause := errors.New("native failure")
	err := NewError(
		ErrorRateLimit,
		"runtime.start",
		"provider asked caller to retry later",
		cause,
		WithDebugDetail("http 429 from provider"),
		WithStatusCode(429),
		WithResponse(map[string]string{"retry-after": "3"}, `{"error":"limited"}`),
		WithProviderModel("openai", "gpt"),
		WithRetryAfter(3*time.Second),
	)

	if !errors.Is(err, cause) {
		t.Fatal("SDKError does not unwrap cause")
	}
	var got *SDKError
	if !ErrorAs(err, &got) {
		t.Fatal("ErrorAs did not find SDKError")
	}
	if got.Category != ErrorRateLimit || got.StatusCode != 429 {
		t.Fatalf("facts = %#v, want rate-limit status 429", got)
	}
	if got.ResponseHeaders["retry-after"] != "3" || got.ResponseBody == "" {
		t.Fatalf("response facts missing: %#v", got)
	}
	if got.Provider != "openai" || got.Model != "gpt" || got.RetryAfter != 3*time.Second {
		t.Fatalf("provider/model/retry-after facts missing: %#v", got)
	}
	if got.DebugDetail != "http 429 from provider" {
		t.Fatalf("DebugDetail = %q", got.DebugDetail)
	}
}
