package agentwrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// CostSource records the provenance of a computed cost value.
type CostSource string

const (
	// CostSourceProviderReported means the runtime or provider reported the
	// cost itself. It is treated as exact.
	CostSourceProviderReported CostSource = "provider_reported"
	// CostSourceModelPriced means the cost was computed locally by pricing
	// token totals against a model rate table. It is an API-equivalent
	// estimate, not necessarily money spent (subscription plans bill
	// separately).
	CostSourceModelPriced CostSource = "model_priced"
	// CostSourceUnpriced means token usage exists but no rate is known for
	// the model, so no cost could be computed.
	CostSourceUnpriced CostSource = "unpriced"
)

const (
	// LiteLLMRatesURL is the public LiteLLM price table used for local cost
	// estimation. It is the same table ccusage prices against.
	LiteLLMRatesURL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"

	// RatesTTL bounds how long a fetched rate snapshot stays fresh.
	RatesTTL = 24 * time.Hour

	// RatesFetchTimeout bounds a single rate-table fetch.
	RatesFetchTimeout = 10 * time.Second

	ratesSnapshotName = "usage-model-rates.json"
	ratesSnapshotVer  = 1
	currencyUSD       = "USD"
)

// Rates freshness states surfaced alongside a resolved rate table.
const (
	RatesStatusFresh       = "fresh"
	RatesStatusCached      = "cached"
	RatesStatusUnavailable = "unavailable"
)

// ModelRates holds USD-per-token rates for one model. Cache rates fall back to
// the plain input rate when a catalog entry omits them, mirroring ccusage.
type ModelRates struct {
	InputPerToken      float64
	OutputPerToken     float64
	CacheReadPerToken  float64
	CacheWritePerToken float64
}

// RateTable maps normalized model names to rates. Lookups on a nil or empty
// table simply miss.
type RateTable struct {
	rates map[string]ModelRates
}

// NormalizeModelName canonicalizes provider-qualified model IDs so that
// "anthropic/claude-sonnet-4-5" and "Claude-Sonnet-4-5" resolve to the same
// key: lowercase, with everything up to the last slash stripped.
func NormalizeModelName(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if idx := strings.LastIndex(normalized, "/"); idx >= 0 {
		normalized = normalized[idx+1:]
	}
	return normalized
}

// Lookup resolves rates for a provider-qualified or bare model name.
func (t RateTable) Lookup(model string) (ModelRates, bool) {
	if len(t.rates) == 0 {
		return ModelRates{}, false
	}
	rate, ok := t.rates[NormalizeModelName(model)]
	return rate, ok
}

// Len reports how many models are priced.
func (t RateTable) Len() int {
	if len(t.rates) == 0 {
		return 0
	}
	return len(t.rates)
}

// PricedUsage is the outcome of pricing token totals. Amount is zero unless
// Source is CostSourceModelPriced; currency is always USD.
type PricedUsage struct {
	Amount   float64
	Currency string
	Source   CostSource
}

// PriceUsage prices token totals against a rate table using the API-equivalent
// formula:
//
//	input*in + cacheRead*readRate + cacheWrite*writeRate + output*outRate
//
// InputTokens must exclude cached tokens (Anthropic-style disjoint buckets,
// which is how OpenCode reports usage). Reasoning tokens are already inside
// OutputTokens and are never charged again. Models without a table entry are
// reported as unpriced rather than estimated at zero cost. A provider-reported
// cost, when available, always takes precedence over this estimate; callers
// should check it first.
func PriceUsage(table RateTable, model string, usage Usage) PricedUsage {
	rate, ok := table.Lookup(model)
	if !ok {
		return PricedUsage{Currency: currencyUSD, Source: CostSourceUnpriced}
	}
	var amount float64
	amount += float64(int64Value(usage.InputTokens)) * rate.InputPerToken
	amount += float64(int64Value(usage.CacheReadTokens)) * rate.CacheReadPerToken
	amount += float64(int64Value(usage.CacheWriteTokens)) * rate.CacheWritePerToken
	amount += float64(int64Value(usage.OutputTokens)) * rate.OutputPerToken
	return PricedUsage{Amount: amount, Currency: currencyUSD, Source: CostSourceModelPriced}
}

// CacheSavings estimates USD saved by cache hits versus re-reading those tokens
// at the plain input rate. The bool is false when the model is unpriced.
func CacheSavings(table RateTable, model string, usage Usage) (float64, bool) {
	rate, ok := table.Lookup(model)
	if !ok {
		return 0, false
	}
	saved := float64(int64Value(usage.CacheReadTokens)) * (rate.InputPerToken - rate.CacheReadPerToken)
	return saved, true
}

// ParseRateTable projects a LiteLLM model_prices_and_context_window document
// into per-token rates. Entries missing either an input or output rate are
// dropped, and tiered/flex/priority/batch variants are ignored.
func ParseRateTable(document []byte) (RateTable, error) {
	var entries map[string]map[string]any
	if err := json.Unmarshal(document, &entries); err != nil {
		return RateTable{}, fmt.Errorf("parse rate catalog: %w", err)
	}
	table := RateTable{rates: make(map[string]ModelRates, len(entries))}
	for name, entry := range entries {
		key := NormalizeModelName(name)
		if key == "" || key == "synthetic" {
			continue
		}
		input, ok := finiteNumber(entry["input_cost_per_token"])
		if !ok {
			continue
		}
		output, ok := finiteNumber(entry["output_cost_per_token"])
		if !ok {
			continue
		}
		read, ok := finiteNumber(entry["cache_read_input_token_cost"])
		if !ok {
			read = input
		}
		write, ok := finiteNumber(entry["cache_creation_input_token_cost"])
		if !ok {
			write = input
		}
		table.rates[key] = ModelRates{
			InputPerToken:      input,
			OutputPerToken:     output,
			CacheReadPerToken:  read,
			CacheWritePerToken: write,
		}
	}
	if len(table.rates) == 0 {
		return RateTable{}, errors.New("rate catalog contained no priced models")
	}
	return table, nil
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func finiteNumber(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return 0, false
		}
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		return parsed, true
	case string:
		var parsed float64
		if err := json.Unmarshal([]byte(typed), &parsed); err != nil {
			return 0, false
		}
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

// RatesInfo reports where the active rate table came from.
type RatesInfo struct {
	Status      string // fresh | cached | unavailable
	Source      string
	FetchedAt   time.Time
	KnownModels int
}

type ratesSnapshot struct {
	Version   int                   `json:"version"`
	FetchedAt time.Time             `json:"fetched_at"`
	Source    string                `json:"source"`
	Rates     map[string]ModelRates `json:"rates"`
}

// RateTableStore resolves model rates with in-memory, on-disk, and network
// layers. Fetches are bounded by RatesTTL; a stale snapshot is preferred over
// failure so pricing degrades to "cached" rather than unpriced offline.
type RateTableStore struct {
	dir    string
	url    string
	client *http.Client
	now    func() time.Time

	mu        sync.Mutex
	table     RateTable
	fetchedAt time.Time
	source    string
	resolved  bool
}

// RateOption configures a RateTableStore.
type RateOption func(*RateTableStore)

// WithRateHTTPClient overrides the HTTP client used for catalog fetches.
func WithRateHTTPClient(client *http.Client) RateOption {
	return func(s *RateTableStore) {
		if client != nil {
			s.client = client
		}
	}
}

// WithRatesURL overrides the catalog URL (primarily for tests).
func WithRatesURL(url string) RateOption {
	return func(s *RateTableStore) {
		if url != "" {
			s.url = url
		}
	}
}

func withRateClock(now func() time.Time) RateOption {
	return func(s *RateTableStore) {
		if now != nil {
			s.now = now
		}
	}
}

// NewRateTableStore builds a store that snapshots rates under dir. An empty
// dir disables persistence (memory-only).
func NewRateTableStore(dir string, options ...RateOption) *RateTableStore {
	store := &RateTableStore{
		dir:    dir,
		url:    LiteLLMRatesURL,
		client: &http.Client{Timeout: RatesFetchTimeout},
		now:    time.Now,
	}
	for _, option := range options {
		option(store)
	}
	return store
}

// Ensure returns a usable rate table, refreshing from the network only when
// no fresh snapshot exists. A stale on-disk snapshot is preferred over total
// failure ("cached"). When nothing can be resolved the returned error is
// non-nil and callers should treat costs as unpriced.
func (s *RateTableStore) Ensure(ctx context.Context) (RateTable, RatesInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.resolved && now.Sub(s.fetchedAt) < RatesTTL {
		return s.table, s.info(RatesStatusFresh), nil
	}
	stale, staleOK := s.loadSnapshot()
	if staleOK && now.Sub(stale.FetchedAt) < RatesTTL {
		s.apply(stale.FetchedAt, RateTable{rates: stale.Rates}, stale.Source)
		return s.table, s.info(RatesStatusFresh), nil
	}
	if table, err := s.fetch(ctx); err == nil {
		fetchedAt := s.now()
		s.apply(fetchedAt, table, s.url)
		s.writeSnapshot(fetchedAt, table)
		return s.table, s.info(RatesStatusFresh), nil
	}
	if staleOK {
		s.apply(stale.FetchedAt, RateTable{rates: stale.Rates}, stale.Source)
		return s.table, s.info(RatesStatusCached), nil
	}
	if s.resolved {
		return s.table, s.info(RatesStatusCached), nil
	}
	return RateTable{}, RatesInfo{Status: RatesStatusUnavailable, Source: s.url}, errors.New("model rates unavailable")
}

// Status reports the last known provenance without triggering I/O.
func (s *RateTableStore) Status() RatesInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resolved {
		return RatesInfo{Status: RatesStatusUnavailable, Source: s.url}
	}
	return s.info(RatesStatusCached)
}

func (s *RateTableStore) apply(fetchedAt time.Time, table RateTable, source string) {
	s.table = table
	s.fetchedAt = fetchedAt
	s.source = source
	s.resolved = len(table.rates) > 0
}

func (s *RateTableStore) info(status string) RatesInfo {
	return RatesInfo{
		Status:      status,
		Source:      s.source,
		FetchedAt:   s.fetchedAt,
		KnownModels: s.table.Len(),
	}
}

func (s *RateTableStore) loadSnapshot() (ratesSnapshot, bool) {
	if s.dir == "" {
		return ratesSnapshot{}, false
	}
	body, err := os.ReadFile(filepath.Join(s.dir, ratesSnapshotName))
	if err != nil {
		return ratesSnapshot{}, false
	}
	var snapshot ratesSnapshot
	if json.Unmarshal(body, &snapshot) != nil || snapshot.Version != ratesSnapshotVer {
		return ratesSnapshot{}, false
	}
	if len(snapshot.Rates) == 0 || snapshot.FetchedAt.IsZero() {
		return ratesSnapshot{}, false
	}
	return snapshot, true
}

func (s *RateTableStore) writeSnapshot(fetchedAt time.Time, table RateTable) {
	if s.dir == "" || len(table.rates) == 0 {
		return
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return
	}
	body, err := json.Marshal(ratesSnapshot{
		Version:   ratesSnapshotVer,
		FetchedAt: fetchedAt.UTC(),
		Source:    s.url,
		Rates:     table.rates,
	})
	if err != nil {
		return
	}
	tmp := filepath.Join(s.dir, ratesSnapshotName+".tmp")
	if os.WriteFile(tmp, body, 0o644) != nil {
		return
	}
	_ = os.Rename(tmp, filepath.Join(s.dir, ratesSnapshotName))
}

func (s *RateTableStore) fetch(ctx context.Context) (RateTable, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return RateTable{}, err
	}
	response, err := s.client.Do(request)
	if err != nil {
		return RateTable{}, fmt.Errorf("fetch rate catalog: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return RateTable{}, fmt.Errorf("fetch rate catalog: unexpected status %d", response.StatusCode)
	}
	document, err := io.ReadAll(http.MaxBytesReader(nil, response.Body, 32<<20))
	if err != nil {
		return RateTable{}, fmt.Errorf("read rate catalog: %w", err)
	}
	return ParseRateTable(document)
}
