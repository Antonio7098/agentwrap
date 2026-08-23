package agentwrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testInt64(v int64) *int64 { return &v }

const testCatalog = `{
  "anthropic/claude-sonnet-4-5": {
    "input_cost_per_token": 3e-6,
    "output_cost_per_token": 1.5e-5,
    "cache_read_input_token_cost": 3e-7,
    "cache_creation_input_token_cost": 3.75e-6
  },
  "openai/gpt-4o": {
    "input_cost_per_token": 2.5e-6,
    "output_cost_per_token": 1e-5
  },
  "broken-model": {
    "input_cost_per_token": 1e-5
  }
}`

func mustParseRateTable(t *testing.T, document string) RateTable {
	t.Helper()
	table, err := ParseRateTable([]byte(document))
	if err != nil {
		t.Fatalf("ParseRateTable: %v", err)
	}
	return table
}

func TestNormalizeModelName(t *testing.T) {
	cases := map[string]string{
		"anthropic/claude-sonnet-4-5": "claude-sonnet-4-5",
		"openai/gpt-4o":               "gpt-4o",
		"GPT-4O":                      "gpt-4o",
		"gpt-4o":                      "gpt-4o",
		"a/b/c/model":                 "model",
	}
	for input, want := range cases {
		if got := NormalizeModelName(input); got != want {
			t.Fatalf("NormalizeModelName(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestParseRateTable(t *testing.T) {
	table := mustParseRateTable(t, testCatalog)
	if table.Len() != 2 {
		t.Fatalf("priced models = %d, want 2 (entries missing output rate dropped)", table.Len())
	}
	rate, ok := table.Lookup("anthropic/claude-sonnet-4-5")
	if !ok {
		t.Fatal("claude-sonnet-4-5 missing from table")
	}
	want := ModelRates{InputPerToken: 3e-6, OutputPerToken: 1.5e-5, CacheReadPerToken: 3e-7, CacheWritePerToken: 3.75e-6}
	if rate != want {
		t.Fatalf("claude rates = %+v, want %+v", rate, want)
	}
	fallback, ok := table.Lookup("gpt-4o")
	if !ok {
		t.Fatal("gpt-4o missing from table")
	}
	// Missing cache rates fall back to the plain input rate.
	if fallback.CacheReadPerToken != fallback.InputPerToken || fallback.CacheWritePerToken != fallback.InputPerToken {
		t.Fatalf("cache fallback = %+v, want input-rate fallback %+v", fallback, fallback)
	}
}

func TestPriceUsageFormula(t *testing.T) {
	table := mustParseRateTable(t, testCatalog)
	usage := Usage{
		InputTokens:      testInt64(100),
		CacheReadTokens:  testInt64(1000),
		CacheWriteTokens: testInt64(10),
		OutputTokens:     testInt64(50),
	}
	priced := PriceUsage(table, "anthropic/claude-sonnet-4-5", usage)
	// 100*3e-6 + 1000*3e-7 + 10*3.75e-6 + 50*1.5e-5
	want := 100*3e-6 + 1000*3e-7 + 10*3.75e-6 + 50*1.5e-5
	if priced.Source != CostSourceModelPriced || priced.Amount != want || priced.Currency != currencyUSD {
		t.Fatalf("priced = %+v, want amount %v model_priced USD", priced, want)
	}
}

func TestPriceUsageUnpricedModel(t *testing.T) {
	table := mustParseRateTable(t, testCatalog)
	priced := PriceUsage(table, "mystery/model", Usage{OutputTokens: testInt64(500)})
	if priced.Source != CostSourceUnpriced || priced.Amount != 0 {
		t.Fatalf("unpriced = %+v, want zero-amount unpriced", priced)
	}
}

func TestPriceUsageNilTokensAreZero(t *testing.T) {
	table := mustParseRateTable(t, testCatalog)
	priced := PriceUsage(table, "openai/gpt-4o", Usage{})
	if priced.Source != CostSourceModelPriced || priced.Amount != 0 {
		t.Fatalf("empty usage = %+v, want zero amount", priced)
	}
}

func TestCacheSavings(t *testing.T) {
	table := mustParseRateTable(t, testCatalog)
	saved, ok := CacheSavings(table, "claude-sonnet-4-5", Usage{CacheReadTokens: testInt64(1000)})
	if !ok || saved != 1000*(3e-6-3e-7) {
		t.Fatalf("savings = %v,%v want %v,true", saved, ok, 1000*(3e-6-3e-7))
	}
	if _, ok := CacheSavings(table, "unknown", Usage{}); ok {
		t.Fatal("savings for unknown model should be unknown")
	}
}

func newTestStore(t *testing.T, dir string, catalog string, fail bool) *RateTableStore {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(catalog))
	}))
	t.Cleanup(server.Close)
	return NewRateTableStore(dir, WithRatesURL(server.URL), withRateClock(func() time.Time { return time.Unix(1_800_000_000, 0) }))
}

func TestRateTableStoreFetchAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	store := newTestStore(t, dir, testCatalog, false)

	table, info, err := store.Ensure(context.Background())
	if err != nil || info.Status != RatesStatusFresh || table.Len() != 2 {
		t.Fatalf("Ensure = %v,%+v,%v", table.Len(), info, err)
	}

	// A second store in the same dir resolves from the snapshot without network.
	offline := newTestStore(t, dir, testCatalog, true)
	table, info, err = offline.Ensure(context.Background())
	if err != nil || info.Status != RatesStatusFresh || table.Len() != 2 {
		t.Fatalf("snapshot Ensure = %v,%+v,%v", table.Len(), info, err)
	}

	body, err := os.ReadFile(filepath.Join(dir, ratesSnapshotName))
	if err != nil {
		t.Fatalf("snapshot file: %v", err)
	}
	var snapshot ratesSnapshot
	if json.Unmarshal(body, &snapshot) != nil || snapshot.Version != ratesSnapshotVer || len(snapshot.Rates) != 2 {
		t.Fatalf("snapshot content invalid: %s", body)
	}
}

func TestRateTableStoreStaleSnapshotServedWhenOffline(t *testing.T) {
	dir := t.TempDir()
	staleTime := time.Now().Add(-48 * time.Hour)
	body, _ := json.Marshal(ratesSnapshot{
		Version:   ratesSnapshotVer,
		FetchedAt: staleTime.UTC(),
		Source:    LiteLLMRatesURL,
		Rates: map[string]ModelRates{
			"gpt-4o": {InputPerToken: 2.5e-6, OutputPerToken: 1e-5, CacheReadPerToken: 2.5e-6, CacheWritePerToken: 2.5e-6},
		},
	})
	if err := os.WriteFile(filepath.Join(dir, ratesSnapshotName), body, 0o644); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t, dir, testCatalog, true)

	table, info, err := store.Ensure(context.Background())
	if err != nil || info.Status != RatesStatusCached || table.Len() != 1 {
		t.Fatalf("stale fallback = %v,%+v,%v; want cached fallback", table.Len(), info, err)
	}
}

func TestRateTableStoreUnavailableWithoutSources(t *testing.T) {
	dir := t.TempDir()
	store := newTestStore(t, dir, testCatalog, true)
	table, info, err := store.Ensure(context.Background())
	if err == nil || info.Status != RatesStatusUnavailable || table.Len() != 0 {
		t.Fatalf("unavailable = %v,%+v,%v", table.Len(), info, err)
	}
	if status := store.Status(); status.Status != RatesStatusUnavailable {
		t.Fatalf("status = %+v", status)
	}
}

func TestFiniteNumberVariants(t *testing.T) {
	var raw map[string]map[string]any
	document := `{"m":{"input_cost_per_token":"0.5","output_cost_per_token":2}}`
	if json.Unmarshal([]byte(document), &raw) != nil {
		t.Fatal("fixture unmarshal")
	}
	if v, ok := finiteNumber(raw["m"]["input_cost_per_token"]); !ok || v != 0.5 {
		t.Fatalf("string number = %v,%v", v, ok)
	}
	if _, ok := finiteNumber(raw["m"]["missing"]); ok {
		t.Fatal("missing should not parse")
	}
	if _, ok := finiteNumber(strings.NewReader("junk")); ok {
		t.Fatal("non-scalar should not parse")
	}
}
