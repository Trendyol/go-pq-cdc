// Package format defines the decoded logical replication message types for PostgreSQL CDC.
package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// Begin represents a decoded logical replication BEGIN message.
type Begin struct {
	CommitTime time.Time
	FinalLSN   pq.LSN
	Xid        uint32
}

// NewBegin parses raw bytes into a Begin message.
func NewBegin(data []byte) (*Begin, error) {
	msg := &Begin{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (b *Begin) decode(data []byte) error {
	skipByte := 1

	if len(data) < 20 {
		return errors.Newf("begin message length must be at least 20 byte, but got %d", len(data))
	}

	b.FinalLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	b.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0) //nolint:gosec // G115: PG timestamp fits int64
	skipByte += 8
	b.Xid = binary.BigEndian.Uint32(data[skipByte:])

	return nil
}
