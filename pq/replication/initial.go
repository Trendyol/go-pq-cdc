package replication

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/metadata"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/jackc/pgx/v5"
	"time"
)

type Initial struct {
	cfg         config.Config
	metadata    metadata.MetaData
	conn        pq.PoolConnection
	publication publication.Publication
}

func NewInitial(metadata metadata.MetaData, conn pq.PoolConnection) *Initial {
	return &Initial{}
}

func (ini *Initial) SetupMetadata(ctx context.Context) error {
	checkpoint := metadata.NewCheckpoint(ini.cfg)

	if !checkpoint.IsRequireChunks() {
		return nil
	}

	if err := ini.metadata.CreateCheckpointTable(ctx); err != nil {
		return err
	}

	if err := ini.metadata.CreateChunkTable(ctx); err != nil {
		return err
	}

	snapshot, err := ini.metadata.ExportSnapshot(ctx)
	if err != nil {
		return err
	}

	checkpoint.InitialSnapshot = snapshot

	if err = ini.metadata.SaveCheckpoint(ctx, checkpoint); err != nil {
		return err // return nil if already exists :D
	}

	tables, err := ini.publication.Tables(ctx)
	if err != nil {
		return err
	}

	tableCounts, err := ini.metadata.TableCounts(ctx, tables)
	if err != nil {
		return err
	}

	// Create chunks
	chunks := metadata.NewChunks(ctx, ini.cfg, snapshot, tableCounts)

	if err = ini.metadata.SaveChunks(ctx, chunks); err != nil {
		return err
	}

	if err = ini.metadata.UpdateChunksCreated(ctx); err != nil {
		return err
	}

	return nil
}

func (ini *Initial) Process(ctx context.Context) error {
	tx, err := ini.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		return err
	}

	defer func() {
		if err = tx.Rollback(ctx); err != nil {
			logger.Error("initial process rollback", "error", err)
		}
	}()

	// TODO: we can set the processing date. if there is no available chunk, then check the captured and not processed yet chunks more than 1 minute :D
	chunk, err := ini.metadata.GetAvailableChunk(ctx, tx)
	if err != nil {
		return err
	}

	if chunk == nil {
		if err = ini.metadata.UpdateInitialDone(ctx, tx); err != nil {
			return err
		}

		return nil
	}

	if _, err = tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", chunk.SnapshotID)); err != nil {
		return err
	}

	tableName := fmt.Sprintf("%s.%s", chunk.Schema, chunk.TableName)
	tableColumns, err := ini.metadata.TableColumns(ctx, []string{tableName}) // move to setup meta data
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE ORDER BY CTID LIMIT $1 OFFSET $2", tableName)
	args := []any{chunk.Limit, chunk.Offset}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var rowValues []any
		rowValues, err = rows.Values()
		if err != nil {
			return err
		}

		msg := format.Insert{
			MessageTime:    time.Now(),
			Decoded:        make(map[string]any),
			TableNamespace: chunk.Schema,
			TableName:      chunk.TableName,
		}
		for i, rowValue := range rowValues {
			msg.Decoded[tableColumns[tableName][uint64(i)]] = rowValue
		}

		// then send message to listenerFunc
	}

	// Remove the chunk if the all data processed
	if err = ini.metadata.DeleteChunk(ctx, tx, chunk.ID); err != nil {
		return err
	}

	return nil
}
