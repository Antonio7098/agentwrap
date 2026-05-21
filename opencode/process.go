package opencode

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

type execProcessRunner struct{}

func (execProcessRunner) Start(ctx context.Context, spec processSpec) (process, error) {
	cmd := exec.CommandContext(ctx, spec.Executable, spec.Args...)
	if spec.WorkDir != "" {
		cmd.Dir = spec.WorkDir
	}
	if len(spec.Env) > 0 {
		cmd.Env = append(os.Environ(), spec.Env...)
	}
	configureProcessGroup(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &execProcess{cmd: cmd, stdout: stdout, stderr: stderr}, nil
}

type execProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.Reader
	once   sync.Once
	result processResult
}

func (p *execProcess) Stdout() io.ReadCloser { return p.stdout }
func (p *execProcess) Stderr() io.Reader     { return p.stderr }

func (p *execProcess) Wait() processResult {
	p.once.Do(func() {
		err := p.cmd.Wait()
		p.result = processResult{Err: err}
		if p.cmd.ProcessState != nil {
			p.result.ExitCode = p.cmd.ProcessState.ExitCode()
		}
		if err == nil {
			return
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			p.result.ExitCode = exitErr.ExitCode()
		}
	})
	return p.result
}

func (p *execProcess) Cancel(ctx context.Context) cleanupResult {
	if p.cmd.Process == nil {
		return cleanupResult{}
	}
	result := cleanupResult{GracefulAttempted: true}
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return result
	}
	if err := signalProcessGroup(p.cmd.Process, false); err != nil && !errors.Is(err, os.ErrProcessDone) {
		result.Err = err
	} else if errors.Is(err, os.ErrProcessDone) {
		return result
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
		return result
	}
	result.ForceAttempted = true
	if err := signalProcessGroup(p.cmd.Process, true); err != nil && !errors.Is(err, os.ErrProcessDone) && result.Err == nil {
		result.Err = err
	}
	return result
}

type limitBuffer struct {
	mu    sync.Mutex
	limit int
	buf   bytes.Buffer
}

func newLimitBuffer(limit int) *limitBuffer {
	return &limitBuffer{limit: limit}
}

func (b *limitBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	written := len(p)
	if b.limit <= 0 {
		return written, nil
	}
	if b.buf.Len()+len(p) <= b.limit {
		_, _ = b.buf.Write(p)
		return written, nil
	}
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		_, _ = b.buf.Write(p[:remaining])
	}
	return written, nil
}

func (b *limitBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
