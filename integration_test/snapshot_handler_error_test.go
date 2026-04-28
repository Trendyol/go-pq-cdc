package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotHandlerErrorPreventsChunkCompletion(t *testing.T) {
	ctx := context.Background()
	tableName := "snapshot_handler_error_test"
	slotName := "snapshot_handler_error_slot"
	cdcCfg := Config
	cdcCfg.Snapshot.Enabled = true
	cdcCfg.Snapshot.Mode = "snapshot_only"
	cdcCfg.Snapshot.ChunkSize = 100
	cdcCfg.Snapshot.Tables = publication.Tables{
		{
			Name:   tableName,
			Schema: "public",
		},
	}
	cdcCfg.SetDefault()
	logger.InitLogger(cdcCfg.Logger.Logger)

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() {
		postgresConn.Close(ctx)
		cleanupSnapshotOnlyTestWithID(t, ctx, tableName, slotName)
	})

	require.NoError(t, createTestTable(ctx, postgresConn, tableName))
	for i := 1; i <= 2; i++ {
		query := fmt.Sprintf("INSERT INTO %s(id, name, age) VALUES(%d, 'User%d', %d)",
			tableName, i, i, 20+i)
		require.NoError(t, pgExec(ctx, postgresConn, query))
	}

	snapshotter, err := snapshot.New(ctx, cdcCfg.Snapshot, cdcCfg.Snapshot.Tables, cdcCfg.DSN(), metric.NewMetric(slotName))
	require.NoError(t, err)
	t.Cleanup(func() {
		snapshotter.Close(ctx)
	})

	require.NoError(t, snapshotter.Prepare(ctx, slotName))

	handlerErr := errors.New("downstream write failed")
	dataEvents := 0
	err = snapshotter.Execute(ctx, func(event *format.Snapshot) error {
		if event.EventType == format.SnapshotEventTypeData {
			dataEvents++
			return handlerErr
		}
		return nil
	}, slotName)
	if err != nil {
		assert.Contains(t, err.Error(), handlerErr.Error())
	}

	assert.Equal(t, 1, dataEvents, "handler error should stop the failed chunk after the first row")

	jobQuery := fmt.Sprintf("SELECT completed, completed_chunks FROM cdc_snapshot_job WHERE slot_name = '%s'", slotName)
	jobResults, err := execQuery(ctx, postgresConn, jobQuery)
	require.NoError(t, err)
	require.Len(t, jobResults[0].Rows, 1)

	jobRow := jobResults[0].Rows[0]
	assert.Equal(t, "f", string(jobRow[0]), "job should not be marked completed")
	assert.Equal(t, "0", string(jobRow[1]), "failed chunk should not count as completed")

	chunkQuery := fmt.Sprintf("SELECT status, rows_processed FROM cdc_snapshot_chunks WHERE slot_name = '%s'", slotName)
	chunkResults, err := execQuery(ctx, postgresConn, chunkQuery)
	require.NoError(t, err)
	require.Len(t, chunkResults[0].Rows, 1)

	chunkRow := chunkResults[0].Rows[0]
	assert.NotEqual(t, "completed", string(chunkRow[0]), "failed chunk should not be completed")
	assert.Equal(t, "0", string(chunkRow[1]), "failed chunk should not record processed rows")
}
