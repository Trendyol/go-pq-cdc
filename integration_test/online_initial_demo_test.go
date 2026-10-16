package integration

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestOnlineInitialDemo(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "online-demo")
	build := exec.Command("go", "build", "-o", binary, "./example/online-initial-poc")
	build.Dir = ".."
	output, err := build.CombinedOutput()
	require.NoError(t, err, string(output))
	source := newOnlinePGFixture(t)
	dest := newOnlinePGFixture(t)
	conn := source.Connect(t)
	print := exec.Command(binary, "-print-source-sql")
	sql, err := print.Output()
	require.NoError(t, err)
	require.Contains(t, string(sql), "CREATE SCHEMA")
	onlineExec(t, conn, string(sql))
	onlineExec(t, conn, `CREATE TABLE demo_rows(id bigint PRIMARY KEY,value text,payload jsonb); ALTER TABLE demo_rows REPLICA IDENTITY FULL; INSERT INTO demo_rows VALUES(1,'snapshot','{"n":9007199254740993}'),(2,'delete','null')`)
	setup := exec.Command(binary, "-setup-target")
	setup.Env = append(os.Environ(), "CDC_ONLINE_TARGET_DSN="+dest.DSN)
	output, err = setup.CombinedOutput()
	require.NoError(t, err, string(output))
	target := &onlineTarget{Conn: dest.Connect(t)}
	cfg := onlineConfig(t, source, "demo_rows")
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	file := filepath.Join(directory, "private-config.yaml")
	require.NoError(t, os.WriteFile(file, data, 0600))
	launch := func() *exec.Cmd {
		cmd := exec.Command(binary, "-config", file)
		cmd.Env = append(os.Environ(), "CDC_ONLINE_TARGET_DSN="+dest.DSN)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		require.NoError(t, cmd.Start())
		t.Cleanup(func() {
			if cmd.ProcessState == nil {
				cmd.Process.Kill()
				cmd.Wait()
			}
		})
		return cmd
	}
	stop := func(cmd *exec.Cmd) {
		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			t.Fatal("example graceful shutdown deadline")
		}
	}
	waitSnapshot := func() {
		onlineEventually(t, func() bool {
			var complete bool
			err := conn.QueryRow(context.Background(), "SELECT status='snapshot_complete' FROM cdc_online.jobs").Scan(&complete)
			return err == nil && complete
		}, 15*time.Second, 10*time.Millisecond, "actual executable snapshot")
	}
	first := launch()
	waitSnapshot()
	var binding string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT slot_binding::text FROM cdc_online.jobs").Scan(&binding))
	oracle := startCDCOracle(t, source, cfg.Publication.Name, binding, false, "demo_rows")
	onlineExec(t, conn, `UPDATE demo_rows SET value='cdc-one',payload='{"n":9007199254740995}' WHERE id=1; UPDATE demo_rows SET value='cdc-two' WHERE id=1; DELETE FROM demo_rows WHERE id=2`)
	waitOnlineFence(t, conn)
	stop(first)
	compareOnlineTable(t, conn, target, "demo_rows")
	_, deleted, token := target.Image(t, online.Table{Schema: "public", Name: "demo_rows"}, 2)
	require.True(t, deleted)
	require.NotZero(t, token.EndLSN)
	second := launch()
	onlineEventually(t, func() bool {
		var generation int
		err := conn.QueryRow(context.Background(), "SELECT generation FROM cdc_online.jobs").Scan(&generation)
		return err == nil && generation == 2
	}, 10*time.Second, 10*time.Millisecond)
	onlineExec(t, conn, `INSERT INTO demo_rows VALUES(2,'reinsert','{"n":9223372036854775807}')`)
	waitOnlineFence(t, conn)
	stop(second)
	compareOnlineTable(t, conn, target, "demo_rows")
	oracle.assert(t, target, 4)
	image, deleted, _ := target.Image(t, online.Table{Schema: "public", Name: "demo_rows"}, 2)
	require.False(t, deleted)
	require.Equal(t, "reinsert", image["value"])
}
