package format

import (
	"encoding/binary"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"time"
)

type Commit struct {
	// Flags currently unused (must be 0).
	Flags uint8
	// CommitLSN is the LSN of the commit.
	CommitLSN pq.LSN
	// TransactionEndLSN is the end LSN of the transaction.
	TransactionEndLSN pq.LSN
	// CommitTime is the commit timestamp of the transaction
	CommitTime time.Time
}

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
	c.CommitTime = time.Unix(int64(binary.BigEndian.Uint64(data[skipByte:])), 0)

	return nil
}
