package replication

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/stretchr/testify/assert"
)

// streamAbortHarness drives dispatchMessage with decoded proto v2 streaming
// messages and collects what reaches the consumer channel.
type streamAbortHarness struct {
	s         *stream
	buf       *messageBuffer
	streamBuf *streamTxBuffer
	out       chan *Message
}

func newStreamAbortHarness() *streamAbortHarness {
	out := make(chan *Message, 100)
	return &streamAbortHarness{
		s:         &stream{},
		buf:       &messageBuffer{outCh: out},
		streamBuf: &streamTxBuffer{},
		out:       out,
	}
}

func (h *streamAbortHarness) dispatch(msg any, lsn uint64) {
	h.s.dispatchMessage(msg, XLogData{WALStart: pq.LSN(lsn)}, h.buf, h.streamBuf)
}

func (h *streamAbortHarness) insert(xid uint32, name string, lsn uint64) {
	h.dispatch(&format.Insert{XID: xid, TableName: name}, lsn)
}

func (h *streamAbortHarness) delivered() []string {
	var names []string
	for {
		select {
		case m := <-h.out:
			names = append(names, m.message.(*format.Insert).TableName)
		default:
			return names
		}
	}
}

func TestStreamAbortSubTransactionKeepsEarlierChanges(t *testing.T) {
	// BEGIN(100); INSERT a; SAVEPOINT(101); INSERT b; INSERT c; ROLLBACK TO SAVEPOINT; INSERT d; COMMIT
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a", 10)
	h.insert(101, "b", 11)
	h.insert(101, "c", 12)
	h.dispatch(&format.StreamStop{}, 13)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 101}, 14)
	h.dispatch(&format.StreamStart{Xid: 100}, 15)
	h.insert(100, "d", 16)
	h.dispatch(&format.StreamStop{}, 17)
	h.dispatch(&format.StreamCommit{Xid: 100, TransactionEndLSN: 99}, 18)

	assert.Equal(t, []string{"a", "d"}, h.delivered())
}

func TestStreamAbortNestedSubTransactions(t *testing.T) {
	// INSERT a(100); SAVEPOINT s1(101); INSERT b; SAVEPOINT s2(102); INSERT c;
	// ROLLBACK TO s2 → abort 102; INSERT e(101); RELEASE s1; ROLLBACK TO s0 … then
	// a second nested case: abort of the outer sub-xid drops the inner one too.
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a", 10)
	h.insert(101, "b", 11)
	h.insert(102, "c", 12)
	h.dispatch(&format.StreamStop{}, 13)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 102}, 14)
	h.dispatch(&format.StreamStart{Xid: 100}, 15)
	h.insert(101, "e", 16)
	h.insert(103, "f", 17)
	h.dispatch(&format.StreamStop{}, 18)
	// PostgreSQL sends the innermost abort first, then the outer one.
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 103}, 19)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 101}, 20)
	h.dispatch(&format.StreamStart{Xid: 100}, 21)
	h.insert(100, "g", 22)
	h.dispatch(&format.StreamStop{}, 23)
	h.dispatch(&format.StreamCommit{Xid: 100, TransactionEndLSN: 99}, 24)

	assert.Equal(t, []string{"a", "g"}, h.delivered())
}

func TestStreamAbortSubTransactionInterleavedTransactions(t *testing.T) {
	// TX-A chunk (with sub-xid), TX-B chunk, TX-A sub-abort: B untouched, A keeps its top-level rows.
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a1", 10)
	h.insert(101, "a-sub", 11)
	h.dispatch(&format.StreamStop{}, 12)
	h.dispatch(&format.StreamStart{Xid: 200}, 13)
	h.insert(200, "b1", 14)
	h.insert(201, "b-sub", 15)
	h.dispatch(&format.StreamStop{}, 16)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 101}, 17)
	h.dispatch(&format.StreamCommit{Xid: 200, TransactionEndLSN: 50}, 18)
	h.dispatch(&format.StreamStart{Xid: 100}, 19)
	h.insert(100, "a2", 20)
	h.dispatch(&format.StreamStop{}, 21)
	h.dispatch(&format.StreamCommit{Xid: 100, TransactionEndLSN: 99}, 22)

	assert.Equal(t, []string{"b1", "b-sub", "a1", "a2"}, h.delivered())
	assert.Empty(t, h.streamBuf.txns)
	assert.Empty(t, h.streamBuf.subXacts)
}

func TestStreamAbortTopLevelDiscardsEverything(t *testing.T) {
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a", 10)
	h.insert(101, "b", 11)
	h.dispatch(&format.StreamStop{}, 12)
	// PostgreSQL aborts the sub-transactions first, then the top-level one.
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 101}, 13)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 100}, 14)

	assert.Empty(t, h.delivered())
	assert.Empty(t, h.streamBuf.txns)
	assert.Empty(t, h.streamBuf.subXacts)
}

func TestStreamAbortUnknownSubTransactionIsNoop(t *testing.T) {
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a", 10)
	h.dispatch(&format.StreamStop{}, 11)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 555}, 12)
	h.dispatch(&format.StreamCommit{Xid: 100, TransactionEndLSN: 99}, 13)

	assert.Equal(t, []string{"a"}, h.delivered())
}

func TestStreamAbortSubTransactionRewritesLastLSN(t *testing.T) {
	// After truncation the surviving last message must carry the transaction-end LSN.
	h := newStreamAbortHarness()
	h.dispatch(&format.StreamStart{Xid: 100}, 1)
	h.insert(100, "a", 10)
	h.insert(101, "b", 11)
	h.dispatch(&format.StreamStop{}, 12)
	h.dispatch(&format.StreamAbort{Xid: 100, SubXid: 101}, 13)
	h.dispatch(&format.StreamCommit{Xid: 100, TransactionEndLSN: 99}, 14)

	m := <-h.out
	assert.Equal(t, "a", m.message.(*format.Insert).TableName)
	assert.Equal(t, int64(99), m.walStart)
}
