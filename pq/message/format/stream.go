package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// StreamStart signals the beginning of a streaming transaction chunk.
// Between StreamStart and StreamStop, DML events belong to an in-progress
// transaction that has not yet been committed.
type StreamStart struct {
	Xid          uint32
	FirstSegment bool
}

func NewStreamStart(data []byte) (*StreamStart, error) {
	// StreamStart message format:
	// Byte1('S')  - message type (already at data[0])
	// Int32       - Xid (4 bytes)
	// Int8        - first_segment flag (1 byte)
	// Total: 1 + 4 + 1 = 6 bytes minimum
	if len(data) < 6 {
		return nil, errors.Newf("stream start message length must be at least 6 bytes, but got %d", len(data))
	}
	return &StreamStart{
		Xid:          binary.BigEndian.Uint32(data[1:]),
		FirstSegment: data[5] == 1,
	}, nil
}

// StreamStop signals the end of a streaming transaction chunk.
// When streaming is enabled, large in-progress transactions are sent
// in chunks bracketed by STREAM START / STREAM STOP.
type StreamStop struct{}

// StreamAbort signals that a streamed transaction has been aborted.
// Any buffered messages from this transaction must be discarded.
type StreamAbort struct {
	Xid    uint32
	SubXid uint32
}

func NewStreamAbort(data []byte) (*StreamAbort, error) {
	// StreamAbort message format:
	// Byte1('A')  - message type (already at data[0])
	// Int32       - Xid (4 bytes)
	// Int32       - SubXid (4 bytes)
	// Total: 1 + 4 + 4 = 9 bytes minimum
	if len(data) < 9 {
		return nil, errors.Newf("stream abort message length must be at least 9 bytes, but got %d", len(data))
	}
	return &StreamAbort{
		Xid:    binary.BigEndian.Uint32(data[1:]),
		SubXid: binary.BigEndian.Uint32(data[5:]),
	}, nil
}

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
