package integration

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func killTestProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	err := cmd.Process.Kill()
	if !errors.Is(err, os.ErrProcessDone) {
		require.NoError(t, err)
	}
}
func requireTestProcessKilled(t *testing.T, err error) {
	t.Helper()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	status, ok := exit.Sys().(syscall.WaitStatus)
	require.True(t, ok)
	require.True(t, status.Signaled())
	require.Equal(t, syscall.SIGKILL, status.Signal())
}
func cleanupTestProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd.ProcessState != nil {
		return
	}
	killTestProcess(t, cmd)
	err := cmd.Wait()
	if err != nil {
		requireTestProcessKilled(t, err)
	}
}
