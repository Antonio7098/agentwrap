package agentwrap

import "testing"

func TestOverallHealthStatusOrdersFailures(t *testing.T) {
	results := []HealthResult{
		{Check: HealthCheckAuthentication, Status: HealthUnknown},
		{Check: HealthCheckProvider, Status: HealthDegraded},
		{Check: HealthCheckRuntimeAvailable, Status: HealthReady},
	}
	if got := OverallHealthStatus(results); got != HealthDegraded {
		t.Fatalf("status = %s, want %s", got, HealthDegraded)
	}
	results = append(results, HealthResult{Check: HealthCheckModel, Status: HealthUnrecoverable})
	if got := OverallHealthStatus(results); got != HealthUnrecoverable {
		t.Fatalf("status = %s, want %s", got, HealthUnrecoverable)
	}
}

func TestRequiredHealthFailureOnlyBlocksRequiredChecks(t *testing.T) {
	report := HealthReport{Results: []HealthResult{
		{Check: HealthCheckAuthentication, Status: HealthUnknown},
		{Check: HealthCheckRuntimeAvailable, Status: HealthReady},
	}}
	if err := RequiredHealthFailure(report, []HealthCheckID{HealthCheckRuntimeAvailable}); err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	err := RequiredHealthFailure(report, []HealthCheckID{HealthCheckAuthentication})
	if err == nil || err.Category != ErrorHealth {
		t.Fatalf("err = %#v, want health failure", err)
	}
	err = RequiredHealthFailure(report, []HealthCheckID{HealthCheckModel})
	if err == nil || err.Category != ErrorHealth || !err.Unrecoverable {
		t.Fatalf("err = %#v, want missing required health failure", err)
	}
}

func TestErrorForHealthStatusClassifiesUnrecoverable(t *testing.T) {
	err := ErrorForHealthStatus(HealthCheckProvider, HealthUnrecoverable, ErrorProviderUnavailable, "missing provider", "provider=x", nil)
	if err.Category != ErrorProviderUnavailable || !err.Unrecoverable || !err.UserActionable || err.Retryable {
		t.Fatalf("bad classification: %#v", err)
	}
}
