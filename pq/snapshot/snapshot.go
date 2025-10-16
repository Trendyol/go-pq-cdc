package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Handler SnapshotHandler is a function that handles snapshot events
type Handler func(event *format.Snapshot) error

type Snapshotter struct {
	conn      pq.Connection // Connection for snapshot operations (with transaction)
	stateConn pq.Connection // Separate connection for state operations (without transaction)
	config    config.SnapshotConfig
	tables    publication.Tables
	typeMap   *pgtype.Map
	metric    metric.Metric
}

func New(snapshotConfig config.SnapshotConfig, tables publication.Tables, conn pq.Connection, stateConn pq.Connection, m metric.Metric) *Snapshotter {
	return &Snapshotter{
		conn:      conn,
		stateConn: stateConn,
		config:    snapshotConfig,
		tables:    tables,
		typeMap:   pgtype.NewMap(),
		metric:    m,
	}
}

func (s *Snapshotter) TakeSnapshot(ctx context.Context, handler Handler, slotName string) error {
	startTime := time.Now()
	logger.Info("snapshot starting", "tables", len(s.tables))

	// Set metrics
	s.metric.SetSnapshotInProgress(true)
	s.metric.SetSnapshotTotalTables(len(s.tables))
	s.metric.SetSnapshotCompletedTables(0)
	defer func() {
		s.metric.SetSnapshotInProgress(false)
		s.metric.SetSnapshotDurationSeconds(time.Since(startTime).Seconds())
	}()

	// Load existing state to resume from checkpoint
	existingState, err := s.LoadState(ctx, slotName)
	if err != nil {
		logger.Debug("no existing snapshot state found, starting fresh")
		existingState = nil
	}

	var resumeFromTable string
	var resumeFromOffset int64
	var totalRows int64
	var currentLSN pq.LSN
	completedTables := 0

	if existingState != nil {
		// Resume from checkpoint - use existing LSN for consistency
		resumeFromTable = existingState.CurrentTable
		resumeFromOffset = existingState.CurrentOffset
		totalRows = existingState.TotalRows
		currentLSN = existingState.LastSnapshotLSN
		logger.Info("resuming snapshot from checkpoint",
			"table", resumeFromTable,
			"offset", resumeFromOffset,
			"totalRows", totalRows,
			"lsn", currentLSN.String())
	} else {
		currentLSN, err = s.getCurrentLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "get current LSN")
		}
		logger.Info("snapshot LSN captured (fresh)", "lsn", currentLSN.String())
	}

	// Send BEGIN marker event
	if err := handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeBegin,
		ServerTime: time.Now().UTC(),
		LSN:        currentLSN,
	}); err != nil {
		return errors.Wrap(err, "send begin marker")
	}

	// Start transaction with REPEATABLE READ isolation
	if err = s.beginTransaction(ctx); err != nil {
		return errors.Wrap(err, "begin transaction")
	}
	defer s.rollbackTransaction(ctx)

	// Process each table
	shouldResume := resumeFromTable != ""
	for _, table := range s.tables {
		tableName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		// Skip tables that were already completed
		if shouldResume && tableName != resumeFromTable {
			logger.Info("skipping already completed table", "table", tableName)
			completedTables++
			continue
		}

		// Determine starting offset for this table
		startOffset := int64(0)
		if shouldResume && tableName == resumeFromTable {
			startOffset = resumeFromOffset
			shouldResume = false // Only resume once
			logger.Info("resuming table from checkpoint", "table", tableName, "offset", startOffset)
		} else {
			logger.Info("snapshot table starting", "schema", table.Schema, "table", table.Name)
		}

		rowCount, err := s.snapshotTable(ctx, table, currentLSN, handler, slotName, startOffset, &totalRows)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("snapshot table %s.%s", table.Schema, table.Name))
		}

		// totalRows is already updated by snapshotTable via pointer
		completedTables++
		s.metric.SetSnapshotCompletedTables(completedTables)
		logger.Info("snapshot table completed", "schema", table.Schema, "table", table.Name, "rows", rowCount)
	}

	// Commit transaction
	if err := s.commitTransaction(ctx); err != nil {
		return errors.Wrap(err, "commit transaction")
	}

	// Send END marker event
	if err := handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        currentLSN,
		TotalRows:  totalRows,
	}); err != nil {
		return errors.Wrap(err, "send end marker")
	}

	logger.Info("snapshot completed", "totalRows", totalRows, "duration", time.Since(startTime))
	return nil
}

// getCurrentLSN gets the current Write-Ahead Log LSN
func (s *Snapshotter) getCurrentLSN(ctx context.Context) (pq.LSN, error) {
	resultReader := s.conn.Exec(ctx, "SELECT pg_current_wal_lsn()")
	results, err := resultReader.ReadAll()
	if err != nil {
		return 0, errors.Wrap(err, "execute pg_current_wal_lsn")
	}

	if err = resultReader.Close(); err != nil {
		return 0, errors.Wrap(err, "close result reader")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return 0, errors.New("no LSN returned")
	}

	lsnStr := string(results[0].Rows[0][0])
	lsn, err := pq.ParseLSN(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "parse LSN")
	}

	return lsn, nil
}

// beginTransaction starts a REPEATABLE READ transaction
func (s *Snapshotter) beginTransaction(ctx context.Context) error {
	resultReader := s.conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "begin transaction")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "close result reader")
	}

	logger.Debug("transaction started")
	return nil
}

// commitTransaction commits the current transaction
func (s *Snapshotter) commitTransaction(ctx context.Context) error {
	resultReader := s.conn.Exec(ctx, "COMMIT")
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "commit transaction")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "close result reader")
	}

	logger.Debug("transaction committed")
	return nil
}

// rollbackTransaction rolls back the current transaction
func (s *Snapshotter) rollbackTransaction(ctx context.Context) {
	resultReader := s.conn.Exec(ctx, "ROLLBACK")
	_, _ = resultReader.ReadAll()
	_ = resultReader.Close()
	logger.Debug("transaction rolled back")
}

// snapshotTable performs chunked reading of a table
func (s *Snapshotter) snapshotTable(
	ctx context.Context,
	table publication.Table,
	lsn pq.LSN,
	handler Handler,
	slotName string,
	startOffset int64,
	globalTotalRows *int64,
) (int64, error) {
	var tableRows int64
	batchNumber := 0
	offset := startOffset

	// Get primary key or use ctid for ordering
	orderByClause, err := s.getOrderByClause(ctx, table)
	if err != nil {
		return 0, errors.Wrap(err, "get order by clause")
	}

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return tableRows, ctx.Err()
		default:
		}

		// Build query with LIMIT and OFFSET
		query := fmt.Sprintf(
			"SELECT * FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
			table.Schema,
			table.Name,
			orderByClause,
			s.config.BatchSize,
			offset,
		)

		logger.Debug("executing snapshot query", "query", query)

		resultReader := s.conn.Exec(ctx, query)
		results, err := resultReader.ReadAll()
		if err != nil {
			return tableRows, errors.Wrap(err, "execute snapshot query")
		}

		if err = resultReader.Close(); err != nil {
			return tableRows, errors.Wrap(err, "close result reader")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			// No more rows
			break
		}

		result := results[0]
		rowCount := len(result.Rows)

		// Process each row
		for i, row := range result.Rows {
			rowData, err := s.parseRow(result.FieldDescriptions, row)
			if err != nil {
				return tableRows, errors.Wrap(err, "parse row")
			}

			isLast := (i == rowCount-1) && (rowCount < s.config.BatchSize)

			// Send data event
			if err := handler(&format.Snapshot{
				EventType:  format.SnapshotEventTypeData,
				Table:      table.Name,
				Schema:     table.Schema,
				Data:       rowData,
				ServerTime: time.Now().UTC(),
				LSN:        lsn,
				IsLast:     isLast,
			}); err != nil {
				return tableRows, errors.Wrap(err, "handle snapshot row")
			}

			tableRows++
			*globalTotalRows = *globalTotalRows + 1
		}

		// Update metrics
		s.metric.SnapshotRowsIncrement(int64(rowCount))

		batchNumber++
		offset += int64(rowCount)

		// Save checkpoint every N batches
		if batchNumber%s.config.CheckpointInterval == 0 {
			state := &SnapshotState{
				SlotName:        slotName,
				LastSnapshotLSN: lsn,
				LastSnapshotAt:  time.Now().UTC(),
				CurrentTable:    fmt.Sprintf("%s.%s", table.Schema, table.Name),
				CurrentOffset:   offset,
				TotalRows:       *globalTotalRows,
				Completed:       false,
			}

			if err := s.SaveState(ctx, state); err != nil {
				logger.Warn("failed to save checkpoint", "error", err)
			} else {
				logger.Debug("checkpoint saved", "table", state.CurrentTable, "offset", offset, "totalRows", *globalTotalRows)
			}
		}

		// If we got fewer rows than batch size, we're done
		if rowCount < s.config.BatchSize {
			break
		}
	}

	return tableRows, nil
}

// getOrderByClause returns the ORDER BY clause for a table
func (s *Snapshotter) getOrderByClause(ctx context.Context, table publication.Table) (string, error) {
	// Try to get primary key columns
	query := fmt.Sprintf(`
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = '%s.%s'::regclass AND i.indisprimary
		ORDER BY a.attnum
	`, table.Schema, table.Name)

	resultReader := s.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return "", errors.Wrap(err, "query primary key")
	}

	if err = resultReader.Close(); err != nil {
		return "", errors.Wrap(err, "close result reader")
	}

	if len(results) > 0 && len(results[0].Rows) > 0 {
		// Build ORDER BY from primary key columns
		var columns []string
		for _, row := range results[0].Rows {
			if len(row) > 0 {
				columns = append(columns, string(row[0]))
			}
		}
		if len(columns) > 0 {
			orderBy := strings.Join(columns, ", ")
			logger.Debug("using primary key for ordering", "table", table.Name, "orderBy", orderBy)
			return orderBy, nil
		}
	}

	// No primary key, use ctid (PostgreSQL internal row identifier)
	logger.Debug("no primary key found, using ctid", "table", table.Name)
	return "ctid", nil
}

// parseRow converts PostgreSQL row data to map with proper type conversion
func (s *Snapshotter) parseRow(fields []pgconn.FieldDescription, row [][]byte) (map[string]any, error) {
	rowData := make(map[string]any)

	for i, field := range fields {
		if i >= len(row) {
			break
		}

		columnName := string(field.Name)
		columnValue := row[i]

		if columnValue == nil {
			rowData[columnName] = nil
			continue
		}

		// Convert to appropriate type using pgtype
		val, err := s.decodeColumnData(columnValue, field.DataTypeOID)
		if err != nil {
			logger.Debug("failed to decode column, using string", "column", columnName, "error", err)
			rowData[columnName] = string(columnValue)
			continue
		}

		rowData[columnName] = val
	}

	return rowData, nil
}

// decodeColumnData decodes PostgreSQL column data using pgtype
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (interface{}, error) {
	if dt, ok := s.typeMap.TypeForOID(dataTypeOID); ok {
		return dt.Codec.DecodeValue(s.typeMap, dataTypeOID, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
