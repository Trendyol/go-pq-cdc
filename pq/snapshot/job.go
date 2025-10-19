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

// Chunk represents a unit of work for snapshot processing
type Chunk struct {
	ClaimedAt   *time.Time
	HeartbeatAt *time.Time
	CompletedAt *time.Time
	SlotName    string
	TableSchema string
	TableName   string
	Status      ChunkStatus
	ClaimedBy   string
	ID          int64
	ChunkIndex  int
	ChunkStart  int64
	ChunkSize   int64
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

		results, err := s.execQuery(ctx, s.jobMetadataConn, query)
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
		fmt.Sscanf(string(row[5]), "%d", &job.TotalChunks)
		fmt.Sscanf(string(row[6]), "%d", &job.CompletedChunks)

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

		results, err := s.execQuery(ctx, s.jobMetadataConn, query)
		if err != nil {
			return errors.Wrap(err, "check job completed")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			isCompleted = false
			return nil
		}

		row := results[0].Rows[0]
		var total, completed int
		fmt.Sscanf(string(row[0]), "%d", &total)
		fmt.Sscanf(string(row[1]), "%d", &completed)

		isCompleted = total > 0 && total == completed
		return nil
	})

	return isCompleted, err
}

// Helper functions

// TODO: buraya bakalım.
func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", s)
}
