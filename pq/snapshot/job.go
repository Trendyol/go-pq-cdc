package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// ChunkStatus represents the status of a chunk
type ChunkStatus string

const (
	ChunkStatusPending    ChunkStatus = "pending"
	ChunkStatusInProgress ChunkStatus = "in_progress"
	ChunkStatusCompleted  ChunkStatus = "completed"
)

// PartitionStrategy defines how a table is partitioned for snapshot
type PartitionStrategy string

const (
	PartitionStrategyIntegerRange PartitionStrategy = "integer_range" // Single integer PK - MIN/MAX range
	PartitionStrategyCTIDBlock    PartitionStrategy = "ctid_block"    // Physical block-based partitioning
	PartitionStrategyOffset       PartitionStrategy = "offset"        // Fallback - LIMIT/OFFSET
)

// Chunk represents a unit of work for snapshot processing
type Chunk struct {
	ClaimedAt   *time.Time
	HeartbeatAt *time.Time
	CompletedAt *time.Time
	RangeEnd    *int64
	RangeStart  *int64

	// CTID block partitioning fields
	BlockStart *int64
	BlockEnd   *int64 // nil for last chunk (no upper bound to catch new rows)

	Status            ChunkStatus
	PartitionStrategy PartitionStrategy
	TableName         string
	ClaimedBy         string
	TableSchema       string
	SlotName          string
	ID                int64
	ChunkIndex        int
	ChunkStart        int64
	ChunkSize         int64
	IsLastChunk       bool // True for the last chunk of a table (no upper bound for CTID)
}

func (c *Chunk) hasRangeBounds() bool {
	return c.RangeStart != nil && c.RangeEnd != nil
}

func (c *Chunk) hasCTIDBlocks() bool {
	return c.BlockStart != nil && c.BlockEnd != nil
}

// Job represents the overall snapshot job metadata
type Job struct {
	StartedAt       time.Time
	SlotName        string
	SnapshotID      string
	SnapshotLSN     pq.LSN
	TotalChunks     int
	CompletedChunks int
	Completed       bool
}

const (
	jobTableName    = "cdc_snapshot_job"
	chunksTableName = "cdc_snapshot_chunks"
)

// loadJob loads the job metadata
func (s *Snapshotter) loadJob(ctx context.Context, slotName string) (*Job, error) {
	var job *Job

	err := s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			SELECT slot_name, snapshot_id, snapshot_lsn, started_at, 
			       completed, total_chunks, completed_chunks
			FROM %s WHERE slot_name = '%s'
		`, jobTableName, slotName)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return errors.Wrap(err, "load job")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			job = nil
			return nil // Not found (not an error)
		}

		row := results[0].Rows[0]
		if len(row) < 7 {
			return errors.New("invalid job row")
		}

		job = &Job{
			SlotName:   string(row[0]),
			SnapshotID: string(row[1]),
		}

		// Parse LSN
		job.SnapshotLSN, err = pq.ParseLSN(string(row[2]))
		if err != nil {
			return errors.Wrap(err, "parse snapshot LSN")
		}

		// Parse timestamp
		job.StartedAt, err = parseTimestamp(string(row[3]))
		if err != nil {
			return errors.Wrap(err, "parse started_at timestamp")
		}

		job.Completed = string(row[4]) == "t" || string(row[4]) == "true"
		if _, err := fmt.Sscanf(string(row[5]), "%d", &job.TotalChunks); err != nil {
			return errors.Wrap(err, "parse total chunks")
		}
		if _, err := fmt.Sscanf(string(row[6]), "%d", &job.CompletedChunks); err != nil {
			return errors.Wrap(err, "parse completed chunks")
		}

		return nil
	})

	return job, err
}

// LoadJob is the public API for connector
func (s *Snapshotter) LoadJob(ctx context.Context, slotName string) (*Job, error) {
	return s.loadJob(ctx, slotName)
}

// checkJobCompleted checks if all chunks are completed
func (s *Snapshotter) checkJobCompleted(ctx context.Context, slotName string) (bool, error) {
	var isCompleted bool

	err := s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			SELECT 
				COUNT(*) as total,
				COUNT(*) FILTER (WHERE status = 'completed') as completed
			FROM %s
			WHERE slot_name = '%s'
		`, chunksTableName, slotName)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return errors.Wrap(err, "check job completed")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			isCompleted = false
			return nil
		}

		row := results[0].Rows[0]
		var total, completed int
		if _, err := fmt.Sscanf(string(row[0]), "%d", &total); err != nil {
			return errors.Wrap(err, "parse total count")
		}
		if _, err := fmt.Sscanf(string(row[1]), "%d", &completed); err != nil {
			return errors.Wrap(err, "parse completed count")
		}

		isCompleted = total > 0 && total == completed
		return nil
	})

	return isCompleted, err
}

// Helper functions

func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		postgresTimestampFormatMicros,
		postgresTimestampFormat,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}
