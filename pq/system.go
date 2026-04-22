package pq

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

// IdentifySystemResult holds the response from the IDENTIFY_SYSTEM replication command.
type IdentifySystemResult struct {
	mu       *sync.RWMutex
	SystemID string
	Database string
	xLogPos  LSN
	Timeline int32
}

// IdentifySystem executes the IDENTIFY_SYSTEM replication command and returns the result.
func IdentifySystem(ctx context.Context, conn Connection) (IdentifySystemResult, error) {
	res, err := ParseIdentifySystem(conn.Exec(ctx, "IDENTIFY_SYSTEM"))
	if err != nil {
		return IdentifySystemResult{}, errors.Wrap(err, "identify system command execute")
	}
	return res, nil
}

// ParseIdentifySystem parses the result of the IDENTIFY_SYSTEM command.
func ParseIdentifySystem(mrr *pgconn.MultiResultReader) (IdentifySystemResult, error) {
	var isr IdentifySystemResult
	results, err := mrr.ReadAll()
	if err != nil {
		return isr, err
	}

	if len(results) != 1 {
		return isr, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return isr, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 4 {
		return isr, fmt.Errorf("expected 4 result columns, got %d", len(row))
	}

	isr.SystemID = string(row[0])
	timeline, err := strconv.ParseInt(string(row[1]), 10, 32)
	if err != nil {
		return isr, fmt.Errorf("failed to parse timeline: %w", err)
	}
	isr.Timeline = int32(timeline)

	isr.xLogPos, err = ParseLSN(string(row[2]))
	if err != nil {
		return isr, fmt.Errorf("failed to parse xlogpos as LSN: %w", err)
	}

	isr.Database = string(row[3])

	isr.mu = &sync.RWMutex{}
	return isr, nil
}

// LoadXLogPos returns the current WAL position.
func (isr *IdentifySystemResult) LoadXLogPos() LSN {
	return isr.xLogPos
}
