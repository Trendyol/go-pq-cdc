package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Direct file descriptors avoid os/exec copying output into a shared buffer.
// The external stdout pipe is owned here, so Wait cannot close its read end.
type capturedTestProcess struct {
	command    *exec.Cmd
	stdout     *os.File
	stderr     *os.File
	done       chan struct{}
	waitResult error
}

func startCapturedTestProcess(t *testing.T, command *exec.Cmd) *capturedTestProcess {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	stderr, err := os.CreateTemp(t.TempDir(), "stderr-")
	if err != nil {
		require.NoError(t, reader.Close())
		t.Fatal(err)
	}
	process := &capturedTestProcess{command: command, stdout: reader, stderr: stderr, done: make(chan struct{})}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, errors.Join(process.stop(ctx), reader.Close(), stderr.Close()))
	})
	command.Stdout, command.Stderr = writer, stderr
	require.NoError(t, command.Start())
	go func() {
		process.waitResult = command.Wait()
		close(process.done)
	}()
	return process
}

func (p *capturedTestProcess) wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.waitResult
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *capturedTestProcess) stop(ctx context.Context) error {
	if p.command.Process == nil {
		return nil
	}
	select {
	case <-p.done:
	default:
		if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
	}
	select {
	case <-p.done:
		var exit *exec.ExitError
		if errors.As(p.waitResult, &exit) {
			return nil // The caller checks intentional panic/kill exit status.
		}
		return p.waitResult
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *capturedTestProcess) stderrText() (string, error) {
	select {
	case <-p.done:
		data, err := os.ReadFile(p.stderr.Name())
		return string(data), err
	default:
		return "", fmt.Errorf("stderr unavailable before process Wait completes")
	}
}

func newBlockedCapturedProcess(t *testing.T, announce bool) *capturedTestProcess {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, errors.Join(reader.Close(), writer.Close())) })
	script := "read -r line"
	if announce {
		script = "printf 'CDC_HELPER_READY\\n'; printf 'stderr-ready\\n' >&2; read -r line"
	}
	command := exec.CommandContext(t.Context(), "/bin/sh", "-c", script)
	command.Stdin = reader
	return startCapturedTestProcess(t, command)
}

func TestCapturedProcessTimeoutJoinsBeforeDiagnostics(t *testing.T) {
	process := newBlockedCapturedProcess(t, true)
	require.NoError(t, waitForHelperReady(process.stdout, 5*time.Second))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, process.wait(ctx), context.Canceled)
	_, err := process.stderrText()
	require.Error(t, err, "a live process must not expose incomplete diagnostics")
	cleanup, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	require.NoError(t, process.stop(cleanup))
	var exit *exec.ExitError
	require.ErrorAs(t, process.wait(cleanup), &exit)
	text, err := process.stderrText()
	require.NoError(t, err)
	require.Contains(t, text, "stderr-ready")
	require.NoError(t, process.stop(cleanup), "cleanup must not call Wait again")
}

func TestCapturedProcessEarlyStopReapsWithoutReadiness(t *testing.T) {
	process := newBlockedCapturedProcess(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, process.stop(ctx))
	var exit *exec.ExitError
	require.ErrorAs(t, process.wait(ctx), &exit)
	_, err := process.stderrText()
	require.NoError(t, err)
	require.NoError(t, process.stop(ctx))
}
