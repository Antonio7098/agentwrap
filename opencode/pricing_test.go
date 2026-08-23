package opencode

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Antonio7098/agentwrap"
)

const pricingCatalog = `{
  "anthropic/claude-sonnet-4-5": {
    "input_cost_per_token": 3e-6,
    "output_cost_per_token": 1.5e-5,
    "cache_read_input_token_cost": 3e-7
  }
}`

func newPricingStore(t *testing.T, dir string) *agentwrap.RateTableStore {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(pricingCatalog))
	}))
	t.Cleanup(server.Close)
	return agentwrap.NewRateTableStore(dir, agentwrap.WithRatesURL(server.URL))
}

func TestPriceFinalUsageModelPriced(t *testing.T) {
	handle := &run{
		context: agentwrap.RuntimeContext{Provider: "anthropic", Model: "anthropic/claude-sonnet-4-5"},
		usage: agentwrap.Usage{
			InputTokens:     int64Ptr(1000),
			CacheReadTokens: int64Ptr(2000),
			OutputTokens:    int64Ptr(500),
		},
		rates: newPricingStore(t, t.TempDir()),
	}
	cost, source := handle.priceFinalUsage()
	if source != agentwrap.CostSourceModelPriced || cost == nil || !cost.Estimate || cost.Currency != "USD" {
		t.Fatalf("cost = %+v, source = %q", cost, source)
	}
	want := 1000*3e-6 + 2000*3e-7 + 500*1.5e-5
	if diff := cost.Amount - want; diff < -1e-12 || diff > 1e-12 {
		t.Fatalf("amount = %v, want %v", cost.Amount, want)
	}
}

func TestPriceFinalUsageUnpriced(t *testing.T) {
	handle := &run{
		context: agentwrap.RuntimeContext{Model: "mystery/model"},
		usage:   agentwrap.Usage{OutputTokens: int64Ptr(10)},
		rates:   newPricingStore(t, t.TempDir()),
	}
	cost, source := handle.priceFinalUsage()
	if cost != nil || source != agentwrap.CostSourceUnpriced {
		t.Fatalf("cost = %+v, source = %q; want unpriced", cost, source)
	}
}

func TestPriceFinalUsageSkippedWithoutTokensOrStore(t *testing.T) {
	handle := &run{
		context: agentwrap.RuntimeContext{Model: "anthropic/claude-sonnet-4-5"},
		rates:   newPricingStore(t, t.TempDir()),
	}
	if _, source := handle.priceFinalUsage(); source != "" {
		t.Fatalf("source = %q, want empty without token usage", source)
	}
	noRates := &run{
		context: agentwrap.RuntimeContext{Model: "anthropic/claude-sonnet-4-5"},
		usage:   agentwrap.Usage{OutputTokens: int64Ptr(10)},
	}
	if _, source := noRates.priceFinalUsage(); source != "" {
		t.Fatalf("source = %q, want empty without rate store", source)
	}
}

func TestRuntimeOptionAttachesRateStore(t *testing.T) {
	store := newPricingStore(t, t.TempDir())
	runtime := NewRuntime(WithRateTableStore(store))
	if runtime.rates != store {
		t.Fatal("rate store not attached")
	}
	if NewRuntime().rates != nil {
		t.Fatal("default runtime should not have a rate store")
	}
}
