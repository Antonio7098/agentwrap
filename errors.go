package agentwrap

import (
	"errors"
	"fmt"
	"time"
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
	Category        ErrorCategory
	Operation       string
	UserDetail      string
	DebugDetail     string
	StatusCode      int
	ResponseHeaders map[string]string
	ResponseBody    string
	Provider        ProviderID
	Model           ModelID
	RuntimeKind     RuntimeKind
	ExitCode        *int
	Signal          string
	NativeType      string
	RetryAfter      time.Duration
	Metadata        map[string]string
	Cause           error
}

func (e *SDKError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Operation == "" {
		return string(e.Category) + ": " + e.UserDetail
	}
	if e.UserDetail == "" {
		return fmt.Sprintf("%s: %s", e.Operation, e.Category)
	}
	return fmt.Sprintf("%s: %s: %s", e.Operation, e.Category, e.UserDetail)
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
		Cause:      cause,
	}
	for _, opt := range opts {
		opt(err)
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

// WithStatusCode records an HTTP-like status code when the runtime exposes one.
func WithStatusCode(status int) ErrorOption {
	return func(err *SDKError) {
		err.StatusCode = status
	}
}

func WithResponse(headers map[string]string, body string) ErrorOption {
	return func(err *SDKError) {
		err.ResponseHeaders = RedactStringMap(headers)
		err.ResponseBody = RedactString(body)
	}
}

func WithProviderModel(provider ProviderID, model ModelID) ErrorOption {
	return func(err *SDKError) {
		err.Provider = provider
		err.Model = model
	}
}

func WithRuntimeKind(kind RuntimeKind) ErrorOption {
	return func(err *SDKError) {
		err.RuntimeKind = kind
	}
}

func WithExitCode(exitCode int) ErrorOption {
	return func(err *SDKError) {
		err.ExitCode = &exitCode
	}
}

func WithSignal(signal string) ErrorOption {
	return func(err *SDKError) {
		err.Signal = signal
	}
}

func WithNativeType(nativeType string) ErrorOption {
	return func(err *SDKError) {
		err.NativeType = nativeType
	}
}

func WithRetryAfter(delay time.Duration) ErrorOption {
	return func(err *SDKError) {
		err.RetryAfter = delay
	}
}

func WithMetadata(metadata map[string]string) ErrorOption {
	return func(err *SDKError) {
		err.Metadata = RedactStringMap(metadata)
	}
}
