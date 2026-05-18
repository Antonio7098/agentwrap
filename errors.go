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
	Category    ErrorCategory
	Operation   string
	UserDetail  string
	DebugDetail string
	// SafeDetail is retained as a compatibility alias for UserDetail.
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
		return string(e.Category) + ": " + e.safeUserDetail()
	}
	if e.safeUserDetail() == "" {
		return fmt.Sprintf("%s: %s", e.Operation, e.Category)
	}
	return fmt.Sprintf("%s: %s: %s", e.Operation, e.Category, e.safeUserDetail())
}

// Unwrap returns the wrapped cause.
func (e *SDKError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ErrorOption customizes a classified SDK error at construction time.
type ErrorOption func(*SDKError)

// NewError constructs a classified SDK error with category defaults.
func NewError(category ErrorCategory, operation, userDetail string, cause error, opts ...ErrorOption) *SDKError {
	err := &SDKError{
		Category:   category,
		Operation:  operation,
		UserDetail: userDetail,
		SafeDetail: userDetail,
		Cause:      cause,
	}
	applyDefaultClassification(err)
	for _, opt := range opts {
		opt(err)
	}
	if err.SafeDetail == "" {
		err.SafeDetail = err.UserDetail
	}
	if err.UserDetail == "" {
		err.UserDetail = err.SafeDetail
	}
	return err
}

// ErrorAs reports whether err contains an SDKError and copies it into target.
func ErrorAs(err error, target **SDKError) bool {
	return errors.As(err, target)
}

// WithDebugDetail records diagnostic detail that is not meant for user display.
func WithDebugDetail(detail string) ErrorOption {
	return func(err *SDKError) {
		err.DebugDetail = detail
	}
}

// WithRetryable overrides whether retry policy may retry this failure.
func WithRetryable(v bool) ErrorOption {
	return func(err *SDKError) {
		err.Retryable = v
	}
}

// WithFallbackable overrides whether fallback policy may switch alternatives.
func WithFallbackable(v bool) ErrorOption {
	return func(err *SDKError) {
		err.Fallbackable = v
	}
}

// WithUserActionable overrides whether the caller can likely fix the failure.
func WithUserActionable(v bool) ErrorOption {
	return func(err *SDKError) {
		err.UserActionable = v
	}
}

// WithUnrecoverable overrides whether policy should treat the failure as final.
func WithUnrecoverable(v bool) ErrorOption {
	return func(err *SDKError) {
		err.Unrecoverable = v
	}
}

func (e *SDKError) safeUserDetail() string {
	if e == nil {
		return ""
	}
	if e.UserDetail != "" {
		return e.UserDetail
	}
	return e.SafeDetail
}

func applyDefaultClassification(err *SDKError) {
	switch err.Category {
	case ErrorConfiguration, ErrorAuthentication, ErrorPermission, ErrorValidation:
		err.UserActionable = true
	case ErrorHealth, ErrorRuntimeUnavailable, ErrorProviderUnavailable, ErrorModelUnavailable:
		err.Retryable = true
		err.Fallbackable = true
	case ErrorRateLimit, ErrorTimeout, ErrorRuntimeExit, ErrorCleanup:
		err.Retryable = true
	case ErrorCancellation:
		err.UserActionable = true
	case ErrorMalformedEvent, ErrorRepairExhausted:
		err.Fallbackable = true
	case ErrorUnknown:
		err.Fallbackable = true
	}
}
