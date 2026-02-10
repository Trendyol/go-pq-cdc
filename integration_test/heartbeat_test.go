package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeatAdvancesLSN(t *testing.T) {
	t.Helper()

	ctx := context.Background()

	// Use base config but customize slot / publication / heartbeat for this test
	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_heartbeat_lsn"

	postgresConn, err := newPostgresConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Ensure base DB objects (books table, drop old publication)
	if !assert.NoError(t, SetupTestDB(ctx, postgresConn, cdcCfg)) {
		t.FailNow()
	}

	// Extend publication with heartbeat table so that heartbeat changes are part of CDC stream
	cdcCfg.Publication.Tables = append(cdcCfg.Publication.Tables,
		publication.Table{
			Name:            "heartbeat_events",
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityFull,
		},
	)

	// Enable heartbeat by specifying the table (library auto-creates it)
	cdcCfg.Heartbeat = config.HeartbeatConfig{
		Table: publication.Table{
			Name:   "heartbeat_events",
			Schema: "public",
		},
		Interval: 2 * time.Second,
	}

	messageCh := make(chan any, 10)
	handlerFunc := func(ctx *replication.ListenerContext) {
		// We don't assert on specific heartbeat events here; just make sure ACKs flow.
		messageCh <- ctx.Message
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Cleanup(func() {
		connector.Close()
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if !assert.NoError(t, connector.WaitUntilReady(waitCtx)) {
		cancel()
		t.FailNow()
	}
	cancel()

	// Capture initial LSNs for the test slot
	initialRestart, initialConfirmed, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Wait long enough for a few heartbeat cycles
	time.Sleep(7 * time.Second)

	finalRestart, finalConfirmed, err := readSlotLSNs(ctx, postgresConn, cdcCfg.Slot.Name)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	// Heartbeat should cause at least confirmed_flush_lsn to move forward.
	assert.NotEmpty(t, initialConfirmed)
	assert.NotEmpty(t, finalConfirmed)
	assert.NotEqualf(t, initialConfirmed, finalConfirmed,
		"expected confirmed_flush_lsn to advance due to heartbeat, got initial=%s final=%s",
		initialConfirmed, finalConfirmed,
	)

	// restart_lsn may move less frequently, but for practical purposes
	// it should also advance when only heartbeat is producing changes.
	assert.NotEmpty(t, initialRestart)
	assert.NotEmpty(t, finalRestart)
}

// readSlotLSNs fetches restart_lsn and confirmed_flush_lsn for a given slot
// as textual LSN representations.
func readSlotLSNs(ctx context.Context, conn pq.Connection, slotName string) (restartLSN string, confirmedLSN string, err error) {
	sql := fmt.Sprintf(
		"SELECT restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s';",
		slotName,
	)

	rr := conn.Exec(ctx, sql)
	results, err := rr.ReadAll()
	if err != nil {
		return "", "", err
	}
	if err = rr.Close(); err != nil {
		return "", "", err
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		return "", "", fmt.Errorf("slot %s not found", slotName)
	}

	row := results[0].Rows[0]
	if len(row) < 2 {
		return "", "", fmt.Errorf("unexpected column count for slot %s", slotName)
	}

	// Values are textual LSNs (e.g. "0/123ABC"), NULL becomes empty string.
	restartLSN = string(row[0])
	confirmedLSN = string(row[1])
	return restartLSN, confirmedLSN, nil
}
