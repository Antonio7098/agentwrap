package agentwrap

import (
	"errors"
	"fmt"
)

// ErrorCategory classifies public SDK failures without string matching.
type ErrorCategory string

const (
	ErrorConfiguration       ErrorCategory = "configuration"
	ErrorHealth              ErrorCategory = "health"
	ErrorRuntimeUnavailable  ErrorCategory = "runtime_unavailable"
	ErrorProviderUnavailable ErrorCategory = "provider_unavailable"
	ErrorModelUnavailable    ErrorCategory = "model_unavailable"
	ErrorAuthentication      ErrorCategory = "authentication"
	ErrorPermission          ErrorCategory = "permission"
	ErrorRateLimit           ErrorCategory = "rate_limit"
	ErrorTimeout             ErrorCategory = "timeout"
	ErrorCancellation        ErrorCategory = "cancellation"
	ErrorMalformedEvent      ErrorCategory = "malformed_event"
	ErrorRuntimeExit         ErrorCategory = "runtime_exit"
	ErrorValidation          ErrorCategory = "validation"
	ErrorRepairExhausted     ErrorCategory = "repair_exhausted"
	ErrorCleanup             ErrorCategory = "cleanup"
	ErrorUnknown             ErrorCategory = "unknown"
)

// SDKError is the public classified error type.
type SDKError struct {
	Category       ErrorCategory
	Operation      string
	SafeDetail     string
	Retryable      bool
	Fallbackable   bool
	UserActionable bool
	Unrecoverable  bool
	Cause          error
}

func (e *SDKError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Operation == "" {
		return string(e.Category) + ": " + e.SafeDetail
	}
	if e.SafeDetail == "" {
		return fmt.Sprintf("%s: %s", e.Operation, e.Category)
	}
	return fmt.Sprintf("%s: %s: %s", e.Operation, e.Category, e.SafeDetail)
}

// Unwrap returns the wrapped cause.
func (e *SDKError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// NewError constructs a classified SDK error.
func NewError(category ErrorCategory, operation, safeDetail string, cause error) *SDKError {
	return &SDKError{
		Category:   category,
		Operation:  operation,
		SafeDetail: safeDetail,
		Cause:      cause,
	}
}

// ErrorAs reports whether err contains an SDKError and copies it into target.
func ErrorAs(err error, target **SDKError) bool {
	return errors.As(err, target)
}
