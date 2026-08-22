package opencode

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/Antonio7098/agentwrap"
)

var _ agentwrap.ModelLister = (*Runtime)(nil)

// maxModelListBytes bounds retained stdout from a model listing probe.
const maxModelListBytes = 512 * 1024

// ListModels enumerates models known to the OpenCode CLI, optionally filtered
// to one provider. It runs the read-only `opencode models` command and never
// starts agent work.
func (r *Runtime) ListModels(ctx context.Context, req agentwrap.ModelsRequest) ([]agentwrap.ModelInfo, error) {
	args := []string{"models"}
	if strings.TrimSpace(string(req.Provider)) != "" {
		args = append(args, string(req.Provider))
	}
	spec := processSpec{Executable: r.executable, Args: args, Env: r.env, WorkDir: req.WorkDir}
	proc, err := r.runner.Start(ctx, spec)
	if err != nil {
		return nil, agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode models", "OpenCode model listing could not start", err)
	}
	type listingOutput struct {
		text string
		err  error
	}
	stdoutCh := make(chan listingOutput, 1)
	stderrCh := make(chan listingOutput, 1)
	go func() {
		defer proc.Stdout().Close()
		data, readErr := io.ReadAll(io.LimitReader(proc.Stdout(), maxModelListBytes+1))
		truncated := len(data) > maxModelListBytes
		if truncated {
			data = data[:maxModelListBytes]
		}
		stdoutCh <- listingOutput{text: string(data), err: readErr}
	}()
	go func() {
		data, _ := io.ReadAll(io.LimitReader(proc.Stderr(), int64(r.stderrLimit)+1))
		if len(data) > r.stderrLimit {
			data = data[:r.stderrLimit]
		}
		stderrCh <- listingOutput{text: string(data)}
	}()
	result := proc.Wait()
	out := <-stdoutCh
	stderr := <-stderrCh
	if out.err != nil && out.err != io.EOF {
		return nil, agentwrap.NewError(agentwrap.ErrorHealth, "opencode models", "OpenCode model listing output could not be read", out.err)
	}
	if result.Err != nil || result.ExitCode != 0 {
		detail := fmt.Sprintf("exit_code=%d stderr=%s", result.ExitCode, agentwrap.RedactString(strings.TrimSpace(stderr.text)))
		sdkErr := agentwrap.NewError(agentwrap.ErrorRuntimeUnavailable, "opencode models", "OpenCode model listing failed", result.Err, agentwrap.WithDebugDetail(detail))
		return nil, sdkErr
	}
	models := parseModelListing(out.text, req.Provider)
	if len(models) == 0 {
		return nil, agentwrap.NewError(agentwrap.ErrorModelUnavailable, "opencode models", "OpenCode reported no available models", nil, agentwrap.WithDebugDetail(agentwrap.RedactString(out.text)))
	}
	return models, nil
}

// parseModelListing reads `opencode models` line output. Each line is a model
// reference of the form provider/model (or bare model when no provider prefix
// is present). Nested model ids such as openrouter/~anthropic/claude-x keep
// their full suffix after the first separator.
func parseModelListing(text string, requested agentwrap.ProviderID) []agentwrap.ModelInfo {
	var models []agentwrap.ModelInfo
	seen := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") || strings.ContainsAny(line, " \t") {
			continue
		}
		id := agentwrap.ModelID(line)
		provider := requested
		if index := strings.Index(line, "/"); index > 0 {
			provider = agentwrap.ProviderID(line[:index])
			id = agentwrap.ModelID(line[index+1:])
		}
		key := string(provider) + "/" + string(id)
		if key == "/" || seen[key] {
			continue
		}
		seen[key] = true
		models = append(models, agentwrap.ModelInfo{Provider: provider, ID: id})
	}
	return models
}
