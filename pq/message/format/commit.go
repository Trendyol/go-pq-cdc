package format

import (
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// Commit represents a decoded logical replication COMMIT message.
type Commit struct {
	CommitTime        time.Time
	CommitLSN         pq.LSN
	TransactionEndLSN pq.LSN
	Flags             uint8
}

// NewCommit parses raw bytes into a Commit message.
func NewCommit(data []byte) (*Commit, error) {
	msg := &Commit{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *Commit) decode(data []byte) error {
	skipByte := 1

	if len(data) < 25 {
		return errors.Newf("commit message length must be at least 25 byte, but got %d", len(data))
	}

	c.Flags = data[skipByte]
	skipByte++
	c.CommitLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	c.TransactionEndLSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
	skipByte += 8
	c.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0) //nolint:gosec // G115: PG timestamp fits int64

	return nil
}
