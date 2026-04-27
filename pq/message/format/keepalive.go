package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"

	"github.com/Trendyol/go-pq-cdc/pq"
)

// PrimaryKeepaliveMessage represents a 'k' keep-alive message sent by the primary server.
// See PostgreSQL replication protocol documentation.
type PrimaryKeepaliveMessage struct {
	ServerTime     time.Time
	ServerWALEnd   pq.LSN
	ReplyRequested bool
}

// NewPrimaryKeepaliveMessage constructs a PrimaryKeepaliveMessage from raw payload (17 bytes, after the leading 'k').
// Format: 8-byte server WAL end + 8-byte server time + 1-byte reply flag.
func NewPrimaryKeepaliveMessage(data []byte) (*PrimaryKeepaliveMessage, error) {
	msg := &PrimaryKeepaliveMessage{}
	if err := msg.decode(data); err != nil {
		return nil, err
	}
	return msg, nil
}

func (p *PrimaryKeepaliveMessage) decode(data []byte) error {
	if len(data) != 17 {
		return errors.Newf("primary keepalive message length must be 17 byte, but got %d", len(data))
	}

	p.ServerWALEnd = pq.LSN(binary.BigEndian.Uint64(data))
	p.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(data[8:])))
	p.ReplyRequested = data[16] != 0
	return nil
}
