package opencode

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/Antonio7098/agentwrap"
)

func TestParseModelListing(t *testing.T) {
	text := strings.Join([]string{
		"openrouter/~anthropic/claude-fable-latest",
		"meta/muse-spark-1.1",
		"meta/muse-spark-1.1",
		"",
		"minimax-coding-plan/MiniMax-M2",
	}, "\n")
	models := parseModelListing(text, "")
	if len(models) != 3 {
		t.Fatalf("models = %v, want 3 entries", models)
	}
	first := models[0]
	if first.Provider != "openrouter" || first.ID != "~anthropic/claude-fable-latest" {
		t.Fatalf("first = %+v, want nested openrouter id preserved", first)
	}
}

func TestParseModelListingBareIDsWithProviderFilter(t *testing.T) {
	models := parseModelListing("alpha\nbeta\n", "vendor")
	if len(models) != 2 {
		t.Fatalf("models = %v, want 2 entries", models)
	}
	for _, model := range models {
		if model.Provider != "vendor" {
			t.Fatalf("provider = %q, want filter applied to bare ids", model.Provider)
		}
	}
}

type stubListProcess struct{ output string }

func (p stubListProcess) Start(_ context.Context, _ processSpec) (process, error) {
	return &stubModelProcess{output: p.output}, nil
}

type stubModelProcess struct {
	output string
	done   bool
}

func (p *stubModelProcess) Stdout() io.ReadCloser { return p }
func (p *stubModelProcess) Read(target []byte) (int, error) {
	if p.done {
		return 0, io.EOF
	}
	p.done = true
	return copy(target, p.output), nil
}
func (p *stubModelProcess) Close() error        { return nil }
func (p *stubModelProcess) Stderr() io.Reader   { return strings.NewReader("") }
func (p *stubModelProcess) Wait() processResult { return processResult{ExitCode: 0} }
func (p *stubModelProcess) Cancel(context.Context) cleanupResult {
	return cleanupResult{}
}

func TestListModelsParsesCLIOutput(t *testing.T) {
	runtime := NewRuntime(withProcessRunner(stubListProcess{output: "openrouter/model-a\nvendorx/model-b\n"}))
	models, err := runtime.ListModels(context.Background(), agentwrap.ModelsRequest{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 2 || models[0].Provider != "openrouter" || models[0].ID != "model-a" || models[1].Provider != "vendorx" || models[1].ID != "model-b" {
		t.Fatalf("models = %+v", models)
	}
}

func TestListModelsFailsWhenNoModels(t *testing.T) {
	runtime := NewRuntime(withProcessRunner(stubListProcess{output: "\n"}))
	if _, err := runtime.ListModels(context.Background(), agentwrap.ModelsRequest{}); err == nil {
		t.Fatal("expected error for empty listing")
	}
}

func TestMiddlewareStackForwardsModelLister(t *testing.T) {
	var runtime agentwrap.Runtime = NewRuntime()
	stack := agentwrap.ObservingRuntime{Runtime: agentwrap.ValidatingRuntime{Runtime: agentwrap.PolicyRunner{Runtime: runtime}}}
	if _, ok := any(stack).(agentwrap.ModelLister); !ok {
		t.Fatal("middleware stack should forward ModelLister")
	}
	bare := agentwrap.ObservingRuntime{Runtime: agentwrap.ValidatingRuntime{Runtime: agentwrap.PolicyRunner{Runtime: bareRuntime{}}}}
	bareLister, ok := any(bare).(agentwrap.ModelLister)
	if !ok {
		t.Fatal("middleware stack should still expose ModelLister")
	}
	if models, err := bareLister.ListModels(context.Background(), agentwrap.ModelsRequest{}); models != nil || err != nil {
		t.Fatalf("expected silent nil,nil for non-lister inner runtime, got %v, %v", models, err)
	}
}

type bareRuntime struct{}

func (bareRuntime) StartRun(context.Context, agentwrap.RunRequest) (agentwrap.Run, error) {
	return nil, nil
}
func (bareRuntime) Capabilities(context.Context) (agentwrap.Capabilities, error) {
	return agentwrap.Capabilities{}, nil
}
