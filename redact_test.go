package agentwrap

import "testing"

func TestRedactStringRemovesCommonSecretForms(t *testing.T) {
	input := "Authorization: Bearer abc123 token=secret-value normal=value"
	got := RedactString(input)
	if got == input || got == "" {
		t.Fatalf("redaction did not change input: %q", got)
	}
	if containsAny(got, "abc123", "secret-value") {
		t.Fatalf("secret leaked: %q", got)
	}
}

func TestRedactMetadataRedactsSecretKeys(t *testing.T) {
	got := RedactMetadata(map[string]any{
		"api_key": "sk-test",
		"note":    "Bearer visible-token",
	})
	if got["api_key"] != "[REDACTED]" {
		t.Fatalf("api_key = %#v", got["api_key"])
	}
	if containsAny(got["note"].(string), "visible-token") {
		t.Fatalf("bearer token leaked: %#v", got["note"])
	}
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if needle != "" && len(value) >= len(needle) {
			for i := 0; i+len(needle) <= len(value); i++ {
				if value[i:i+len(needle)] == needle {
					return true
				}
			}
		}
	}
	return false
}
