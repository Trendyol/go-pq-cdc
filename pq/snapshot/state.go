package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// SnapshotState represents the state of snapshot operation
type SnapshotState struct {
	SlotName        string
	LastSnapshotLSN pq.LSN
	LastSnapshotAt  time.Time
	CurrentTable    string // Currently processing table (for recovery)
	CurrentOffset   int64  // Current offset in table (for recovery)
	TotalRows       int64  // Total rows processed
	Completed       bool
}

const stateTableName = "cdc_snapshot_state"

// initStateTable creates the state table if not exists
func (s *Snapshotter) initStateTable(ctx context.Context) error {
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			slot_name TEXT PRIMARY KEY,
			last_snapshot_lsn TEXT,
			last_snapshot_at TIMESTAMP,
			current_table TEXT,
			current_offset BIGINT DEFAULT 0,
			total_rows BIGINT DEFAULT 0,
			completed BOOLEAN DEFAULT FALSE
		)
	`, stateTableName)

	resultReader := s.stateConn.Exec(ctx, createTableSQL)
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "create snapshot state table")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "close result reader")
	}

	logger.Debug("snapshot state table initialized")
	return nil
}

// SaveState saves the snapshot state to PostgreSQL metadata table
func (s *Snapshotter) SaveState(ctx context.Context, state *SnapshotState) error {
	if err := s.initStateTable(ctx); err != nil {
		return err
	}

	// Convert LSN to string
	lsnStr := state.LastSnapshotLSN.String()

	// PostgreSQL timestamp format (without timezone)
	pgTimestampFormat := "2006-01-02 15:04:05"
	timestampStr := state.LastSnapshotAt.Format(pgTimestampFormat)

	// Execute upsert query (using separate state connection - not in transaction!)
	resultReader := s.stateConn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (
			slot_name, 
			last_snapshot_lsn, 
			last_snapshot_at, 
			current_table, 
			current_offset,
			total_rows,
			completed
		) VALUES ('%s', '%s', '%s', '%s', %d, %d, %t)
		ON CONFLICT (slot_name) DO UPDATE SET
			last_snapshot_lsn = EXCLUDED.last_snapshot_lsn,
			last_snapshot_at = EXCLUDED.last_snapshot_at,
			current_table = EXCLUDED.current_table,
			current_offset = EXCLUDED.current_offset,
			total_rows = EXCLUDED.total_rows,
			completed = EXCLUDED.completed
	`, stateTableName,
		state.SlotName,
		lsnStr,
		timestampStr,
		state.CurrentTable,
		state.CurrentOffset,
		state.TotalRows,
		state.Completed,
	))

	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "save snapshot state")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "close result reader")
	}

	logger.Debug("snapshot state saved", "slotName", state.SlotName, "table", state.CurrentTable, "offset", state.CurrentOffset)
	return nil
}

// LoadState loads the snapshot state from PostgreSQL metadata table
func (s *Snapshotter) LoadState(ctx context.Context, slotName string) (*SnapshotState, error) {
	if err := s.initStateTable(ctx); err != nil {
		return nil, err
	}

	selectSQL := fmt.Sprintf(`
		SELECT 
			slot_name, 
			last_snapshot_lsn, 
			last_snapshot_at, 
			current_table, 
			current_offset,
			total_rows,
			completed
		FROM %s 
		WHERE slot_name = '%s'
	`, stateTableName, slotName)

	resultReader := s.stateConn.Exec(ctx, selectSQL)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "load snapshot state")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "close result reader")
	}

	// No state found
	if len(results) == 0 || len(results[0].Rows) == 0 {
		logger.Debug("no snapshot state found", "slotName", slotName)
		return nil, nil
	}

	row := results[0].Rows[0]
	if len(row) < 7 {
		return nil, errors.New("invalid snapshot state row")
	}

	state := &SnapshotState{
		SlotName:     string(row[0]),
		CurrentTable: string(row[3]),
	}

	// Parse LSN
	if len(row[1]) > 0 {
		state.LastSnapshotLSN, err = pq.ParseLSN(string(row[1]))
		if err != nil {
			return nil, errors.Wrap(err, "parse LSN")
		}
	}

	// Parse timestamp - PostgreSQL format
	if len(row[2]) > 0 {
		// PostgreSQL TIMESTAMP format: "2006-01-02 15:04:05"
		pgTimestampFormat := "2006-01-02 15:04:05"
		state.LastSnapshotAt, err = time.Parse(pgTimestampFormat, string(row[2]))
		if err != nil {
			// Try with microseconds if present
			pgTimestampFormatMicro := "2006-01-02 15:04:05.999999"
			state.LastSnapshotAt, err = time.Parse(pgTimestampFormatMicro, string(row[2]))
			if err != nil {
				return nil, errors.Wrap(err, fmt.Sprintf("parse timestamp: %s", string(row[2])))
			}
		}
	}

	// Parse offset
	if len(row[4]) > 0 {
		fmt.Sscanf(string(row[4]), "%d", &state.CurrentOffset)
	}

	// Parse total rows
	if len(row[5]) > 0 {
		fmt.Sscanf(string(row[5]), "%d", &state.TotalRows)
	}

	// Parse completed
	if len(row[6]) > 0 {
		state.Completed = string(row[6]) == "t" || string(row[6]) == "true"
	}

	logger.Debug("snapshot state loaded", "state", state)
	return state, nil
}
