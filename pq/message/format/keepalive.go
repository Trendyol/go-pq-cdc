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
	ServerWALEnd   pq.LSN    // server's current WAL end position
	ServerTime     time.Time // server clock at the time of sending (microseconds since 2000-01-01)
	ReplyRequested bool      // whether the client should reply immediately
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

// pgTimeToTime converts microseconds since 2000-01-01 (Y2K) to time.Time (UTC).
func pgTimeToTime(microSecSinceY2K int64) time.Time {
	const microSecFromUnixEpochToY2K = int64(946684800) // Unix timestamp of 2000-01-01
	return time.Unix(microSecFromUnixEpochToY2K+(microSecSinceY2K/1_000_000), (microSecSinceY2K%1_000_000)*1_000).UTC()
}
