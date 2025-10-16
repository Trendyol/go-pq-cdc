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
	ServerTime time.Time
	Data       map[string]any
	EventType  SnapshotEventType
	Table      string
	Schema     string
	LSN        pq.LSN
	TotalRows  int64
	IsLast     bool
}
