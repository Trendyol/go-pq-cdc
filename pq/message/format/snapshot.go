package format

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
)

// SnapshotEventType represents the type of snapshot event
type SnapshotEventType string

const (
	SnapshotEventTypeBegin SnapshotEventType = "BEGIN" // Snapshot started
	SnapshotEventTypeData  SnapshotEventType = "DATA"  // Row data
	SnapshotEventTypeEnd   SnapshotEventType = "END"   // Snapshot completed
)

// Snapshot represents a snapshot event
type Snapshot struct {
	EventType  SnapshotEventType // Event type (BEGIN, DATA, END)
	Table      string            // Table name
	Schema     string            // Schema name
	Data       map[string]any    // Row data (only for DATA events)
	ServerTime time.Time         // Event timestamp
	LSN        pq.LSN            // LSN at snapshot start
	IsLast     bool              // Last row of table (only for DATA events)
	TotalRows  int64             // Total rows processed (for END event)
}
