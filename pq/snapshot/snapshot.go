package snapshot

import (
	"context"
	"fmt"
	"strings"

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
	conn      pq.Connection
	stateConn pq.Connection
	metric    metric.Metric
	typeMap   *pgtype.Map
	tables    publication.Tables
	config    config.SnapshotConfig
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

// getCurrentLSN gets the current Write-Ahead Log LSN
func (s *Snapshotter) getCurrentLSN(ctx context.Context) (pq.LSN, error) {
	results, err := s.execQuery(ctx, s.conn, "SELECT pg_current_wal_lsn()")
	if err != nil {
		return 0, errors.Wrap(err, "execute pg_current_wal_lsn")
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
	return s.execSQL(ctx, s.conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
}

// commitTransaction commits the current transaction
func (s *Snapshotter) commitTransaction(ctx context.Context) error {
	return s.execSQL(ctx, s.conn, "COMMIT")
}

// rollbackTransaction rolls back the current transaction
func (s *Snapshotter) rollbackTransaction(ctx context.Context) {
	_ = s.execSQL(ctx, s.conn, "ROLLBACK")
	logger.Debug("transaction rolled back")
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

	results, err := s.execQuery(ctx, s.conn, query)
	if err != nil {
		return "", errors.Wrap(err, "query primary key")
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
