package snapshot

import (
	"context"
	"fmt"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/go-playground/errors"
)

// exportSnapshot exports the current transaction snapshot for use by other connections
// Uses snapshotTxConn which keeps transaction open until all chunks are processed
// Requires: REPLICATION privilege, wal_level=logical, max_replication_slots>0
func (s *Snapshotter) exportSnapshot(ctx context.Context, exportSnapshotConn pq.Connection) (string, error) {
	var snapshotID string

	err := s.retryDBOperation(ctx, func() error {
		results, err := s.execQuery(ctx, exportSnapshotConn, "SELECT pg_export_snapshot()")
		if err != nil {
			if strings.Contains(err.Error(), "permission denied") {
				return errors.New("pg_export_snapshot requires REPLICATION privilege. Run: ALTER USER your_user WITH REPLICATION")
			}
			if strings.Contains(err.Error(), "wal_level") {
				return errors.New("pg_export_snapshot requires wal_level='logical'. Set in postgresql.conf and restart")
			}
			return errors.Wrap(err, "export snapshot")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
			return errors.New("no snapshot ID returned")
		}

		snapshotID = string(results[0].Rows[0][0])
		return nil
	})

	return snapshotID, err
}

// setTransactionSnapshot sets the current transaction to use an exported snapshot
func (s *Snapshotter) setTransactionSnapshot(ctx context.Context, conn pq.Connection, snapshotID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID)
		if err := s.execSQL(ctx, conn, query); err != nil {
			return errors.Wrap(err, "set transaction snapshot")
		}

		logger.Debug("[worker] transaction snapshot set", "snapshotID", snapshotID)
		return nil
	})
}
