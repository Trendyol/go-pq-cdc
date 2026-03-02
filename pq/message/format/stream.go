package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// StreamStop signals the end of a streaming transaction chunk.
// When streaming is enabled, large in-progress transactions are sent
// in chunks bracketed by STREAM START / STREAM STOP. Any buffered
// message must be flushed when this marker arrives.
type StreamStop struct{}

// StreamAbort signals that a streamed transaction has been aborted.
// Any buffered messages from this transaction must be discarded.
type StreamAbort struct{}

// StreamCommit signals the final commit of a streamed transaction.
// It carries the same LSN information as a regular Commit message,
// allowing the last buffered message's WAL position to be rewritten
// to the transaction end LSN.
type StreamCommit struct {
	CommitTime        time.Time
	CommitLSN         pq.LSN
	TransactionEndLSN pq.LSN
	Xid               uint32
	Flags             uint8
}

func NewStreamCommit(data []byte) (*StreamCommit, error) {
	msg := &StreamCommit{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (sc *StreamCommit) decode(data []byte) error {
	skipByte := 1

	// StreamCommit message format:
	// Byte1('c')  - already at data[0]
	// Int32       - Xid (4 bytes)
	// Int8        - Flags (1 byte)
	// Int64       - CommitLSN (8 bytes)
	// Int64       - TransactionEndLSN (8 bytes)
	// Int64       - CommitTime (8 bytes)
	// Total: 1 + 4 + 1 + 8 + 8 + 8 = 30 bytes minimum
	if len(data) < 30 {
		return errors.Newf("stream commit message length must be at least 30 bytes, but got %d", len(data))
	}

	sc.Xid = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4
	sc.Flags = data[skipByte]
	skipByte++
	sc.CommitLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	sc.TransactionEndLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	sc.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0)

	return nil
}
