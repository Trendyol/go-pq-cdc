package online

import (
	"context"
	"github.com/Trendyol/go-pq-cdc/pq"
	"sync/atomic"
	"time"
)

type ObservationPhase string

const (
	CaptureStarted      ObservationPhase = "CAPTURE_STARTED"
	LOWRecorded         ObservationPhase = "LOW_RECORDED"
	ReadStarted         ObservationPhase = "READ_STARTED"
	ReadResult          ObservationPhase = "READ_RESULT"
	HIGHRecorded        ObservationPhase = "HIGH_RECORDED"
	ReadsDelivered      ObservationPhase = "READS_DELIVERED"
	CompletionCommitted ObservationPhase = "COMPLETION_COMMITTED"
	ACKAdvanced         ObservationPhase = "ACK_ADVANCED"
)

// Observation is immutable bounded metadata, never row payload or connection.
// COMPLETION_COMMITTED is after the metadata transaction's successful response
// and before ACK. Observer errors cannot roll that commit back. ACK_ADVANCED is
// the caller-certified reader frontier, not proof the server received feedback.
type Observation struct {
	Phase            ObservationPhase
	JobID, AttemptID string
	Generation       int64
	Table            Table
	EndLSN           pq.LSN
	SelectedRows     int
	HasLastKey       bool
	LastKey          int64
}
type Observer func(context.Context, Observation) error

type Stats struct {
	PeakChunkRawBytes                int64
	PeakReadLatency                  time.Duration
	ReadQueries                      int64
	PeakTransactionEncodedBytes      int64
	PeakQueueAndAssemblyEncodedBytes int64
	PeakRelationEncodedBytes         int64
	BufferedEncodedBytes             int64
	SafeACK                          pq.LSN
	FeedbackWrites                   uint64
	LastFeedback                     time.Time
}
type Counters struct{ chunk, latency, queries atomic.Int64 }

func (s *Counters) RecordRead(bytes int64, elapsed time.Duration) {
	s.queries.Add(1)
	peak(&s.chunk, bytes)
	peak(&s.latency, int64(elapsed))
}
func peak(counter *atomic.Int64, n int64) {
	for {
		old := counter.Load()
		if n <= old || counter.CompareAndSwap(old, n) {
			return
		}
	}
}
func (s *Counters) Snapshot() Stats {
	return Stats{PeakChunkRawBytes: s.chunk.Load(), PeakReadLatency: time.Duration(s.latency.Load()), ReadQueries: s.queries.Load()}
}
