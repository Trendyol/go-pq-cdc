package snapshot

import (
	"context"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/go-playground/errors"
	"strings"
)

// exportSnapshot exports the current transaction snapshot for use by other connections
// Requires: REPLICATION privilege, wal_level=logical, max_replication_slots>0
func (s *Snapshotter) exportSnapshot(ctx context.Context) (string, error) {
	results, err := s.execQuery(ctx, s.conn, "SELECT pg_export_snapshot()")
	if err != nil {
		// Provide helpful error message if prerequisites not met
		if strings.Contains(err.Error(), "permission denied") {
			return "", errors.New("pg_export_snapshot requires REPLICATION privilege. Run: ALTER USER your_user WITH REPLICATION")
		}
		if strings.Contains(err.Error(), "wal_level") {
			return "", errors.New("pg_export_snapshot requires wal_level='logical'. Set in postgresql.conf and restart")
		}
		return "", errors.Wrap(err, "export snapshot")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return "", errors.New("no snapshot ID returned")
	}

	snapshotID := string(results[0].Rows[0][0])
	logger.Info("snapshot exported", "snapshotID", snapshotID)
	return snapshotID, nil
}

// setTransactionSnapshot sets the current transaction to use an exported snapshot
func (s *Snapshotter) setTransactionSnapshot(ctx context.Context, snapshotID string) error {
	query := fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID)
	if err := s.execSQL(ctx, s.conn, query); err != nil {
		return errors.Wrap(err, "set transaction snapshot")
	}

	logger.Debug("transaction snapshot set", "snapshotID", snapshotID)
	return nil
}
