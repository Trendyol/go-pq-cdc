package replication

import (
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAbortRelOID uint32 = 1001

// streamAbortHarness drives handleXLogData with wire-encoded proto v2 streaming
// frames and collects what reaches the consumer channel.
type streamAbortHarness struct {
	t         *testing.T
	s         *stream
	buf       *messageBuffer
	streamBuf *streamTxBuffer
	out       chan *Message
}

func newStreamAbortHarness(t *testing.T, spillCfg slot.StreamSpillConfig) *streamAbortHarness {
	t.Helper()
	out := make(chan *Message, 100)
	sb, err := newStreamTxBuffer(spillCfg, "abort_test_slot", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sb.close())
	})
	return &streamAbortHarness{
		t: t,
		s: &stream{
			mu: &sync.RWMutex{},
			relation: map[uint32]*format.Relation{
				testAbortRelOID: {
					OID:       testAbortRelOID,
					Namespace: "public",
					Name:      "books",
					Columns: []tuple.RelationColumn{
						{Name: "name", DataType: pgtype.TextOID},
					},
				},
			},
		},
		buf:       &messageBuffer{outCh: out},
		streamBuf: sb,
		out:       out,
	}
}

func runAbortModes(t *testing.T, fn func(t *testing.T, h *streamAbortHarness)) {
	t.Helper()
	t.Run("in_memory", func(t *testing.T) {
		fn(t, newStreamAbortHarness(t, slot.StreamSpillConfig{}))
	})
	t.Run("spilled", func(t *testing.T) {
		fn(t, newStreamAbortHarness(t, slot.StreamSpillConfig{
			Enabled:        true,
			MaxMemoryBytes: 1,
			SpillDir:       t.TempDir(),
		}))
	})
}

func (h *streamAbortHarness) feedWire(walData []byte, lsn uint64) {
	h.t.Helper()
	raw := encodeXLogData(pq.LSN(lsn), pq.LSN(lsn), time.Unix(1_700_000_000, 0).UTC(), walData)
	require.NoError(h.t, h.s.handleXLogData(raw, h.buf, h.streamBuf))
}

func (h *streamAbortHarness) insert(xid uint32, rowName string, lsn uint64) {
	h.feedWire(encodeStreamedInsert(xid, testAbortRelOID, wireTextCol(rowName)), lsn)
}

func (h *streamAbortHarness) delivered() []string {
	var names []string
	for {
		select {
		case m := <-h.out:
			names = append(names, m.message.(*format.Insert).Decoded["name"].(string))
		default:
			return names
		}
	}
}

func TestStreamAbortSubTransactionKeepsEarlierChanges(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		// BEGIN(100); INSERT a; SAVEPOINT(101); INSERT b; INSERT c; ROLLBACK TO SAVEPOINT; INSERT d; COMMIT
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a", 10)
		h.insert(101, "b", 11)
		h.insert(101, "c", 12)
		h.feedWire(encodeStreamStop(), 13)
		h.feedWire(encodeStreamAbort(100, 101), 14)
		h.feedWire(encodeStreamStart(100, 0), 15)
		h.insert(100, "d", 16)
		h.feedWire(encodeStreamStop(), 17)
		h.feedWire(encodeStreamCommit(100, 90, 99), 18)

		assert.Equal(t, []string{"a", "d"}, h.delivered())
	})
}

func TestStreamAbortNestedSubTransactions(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a", 10)
		h.insert(101, "b", 11)
		h.insert(102, "c", 12)
		h.feedWire(encodeStreamStop(), 13)
		h.feedWire(encodeStreamAbort(100, 102), 14)
		h.feedWire(encodeStreamStart(100, 0), 15)
		h.insert(101, "e", 16)
		h.insert(103, "f", 17)
		h.feedWire(encodeStreamStop(), 18)
		// PostgreSQL sends the innermost abort first, then the outer one.
		h.feedWire(encodeStreamAbort(100, 103), 19)
		h.feedWire(encodeStreamAbort(100, 101), 20)
		h.feedWire(encodeStreamStart(100, 0), 21)
		h.insert(100, "g", 22)
		h.feedWire(encodeStreamStop(), 23)
		h.feedWire(encodeStreamCommit(100, 90, 99), 24)

		assert.Equal(t, []string{"a", "g"}, h.delivered())
	})
}

func TestStreamAbortSubTransactionInterleavedTransactions(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		// TX-A chunk (with sub-xid), TX-B chunk, TX-A sub-abort: B untouched, A keeps its top-level rows.
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a1", 10)
		h.insert(101, "a-sub", 11)
		h.feedWire(encodeStreamStop(), 12)
		h.feedWire(encodeStreamStart(200, 1), 13)
		h.insert(200, "b1", 14)
		h.insert(201, "b-sub", 15)
		h.feedWire(encodeStreamStop(), 16)
		h.feedWire(encodeStreamAbort(100, 101), 17)
		h.feedWire(encodeStreamCommit(200, 45, 50), 18)
		h.feedWire(encodeStreamStart(100, 0), 19)
		h.insert(100, "a2", 20)
		h.feedWire(encodeStreamStop(), 21)
		h.feedWire(encodeStreamCommit(100, 90, 99), 22)

		assert.Equal(t, []string{"b1", "b-sub", "a1", "a2"}, h.delivered())
		assert.Empty(t, h.streamBuf.txns)
		assert.Zero(t, h.streamBuf.totalMemBytes)
		assert.Zero(t, h.streamBuf.totalDiskBytes)
	})
}

func TestStreamAbortTopLevelDiscardsEverything(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a", 10)
		h.insert(101, "b", 11)
		h.feedWire(encodeStreamStop(), 12)
		// PostgreSQL aborts the sub-transactions first, then the top-level one.
		h.feedWire(encodeStreamAbort(100, 101), 13)
		h.feedWire(encodeStreamAbort(100, 100), 14)

		assert.Empty(t, h.delivered())
		assert.Empty(t, h.streamBuf.txns)
		assert.Zero(t, h.streamBuf.totalMemBytes)
		assert.Zero(t, h.streamBuf.totalDiskBytes)
	})
}

func TestStreamAbortUnknownSubTransactionIsNoop(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a", 10)
		h.feedWire(encodeStreamStop(), 11)
		h.feedWire(encodeStreamAbort(100, 555), 12)
		h.feedWire(encodeStreamCommit(100, 90, 99), 13)

		assert.Equal(t, []string{"a"}, h.delivered())
	})
}

func TestStreamAbortSubTransactionRewritesLastLSN(t *testing.T) {
	runAbortModes(t, func(t *testing.T, h *streamAbortHarness) {
		// After truncation the surviving last message must carry the transaction-end LSN.
		h.feedWire(encodeStreamStart(100, 1), 1)
		h.insert(100, "a", 10)
		h.insert(101, "b", 11)
		h.feedWire(encodeStreamStop(), 12)
		h.feedWire(encodeStreamAbort(100, 101), 13)
		h.feedWire(encodeStreamCommit(100, 90, 99), 14)

		m := <-h.out
		assert.Equal(t, "a", m.message.(*format.Insert).Decoded["name"])
		assert.Equal(t, int64(99), m.walStart)
	})
}
