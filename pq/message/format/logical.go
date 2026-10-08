package format

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
)

// LogicalDecodingMessage is a pgoutput logical decoding message (type 'M'),
// produced by pg_logical_emit_message. Wire layout, from logicalmsg_read:
//
//	Byte1('M')   message type
//	Int8         flags (bit 0 = transactional)
//	Int64        LSN, only when the message is transactional
//	String       prefix, null terminated
//	Int32        content length
//	Bytes        content
//
// There is no commit timestamp on the wire. MessageTime is the server time of
// the WAL record. Inside a streamed transaction (proto version 2) a 4 byte XID
// precedes the body, matching the other streamed decoders.
type LogicalDecodingMessage struct {
	MessageTime time.Time
	Flags       uint8
	// LSN is the WAL position pgoutput reports for a transactional message.
	// Zero for a non-transactional message, which has no LSN on the wire.
	LSN     pq.LSN
	Prefix  string
	Content []byte
	XID     uint32
}

func NewLogicalDecodingMessage(data []byte, streamedTransaction bool, serverTime time.Time) (*LogicalDecodingMessage, error) {
	msg := &LogicalDecodingMessage{MessageTime: serverTime}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *LogicalDecodingMessage) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 13 {
			return errors.Newf("streamed transaction logical decoding message length must be at least 13 bytes, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < skipByte+2 {
		return errors.Newf("logical decoding message length must be at least %d bytes, but got %d", skipByte+2, len(data))
	}

	m.Flags = data[skipByte]
	skipByte++

	if m.Flags&1 != 0 {
		if len(data) < skipByte+8 {
			return errors.Newf("transactional logical decoding message is missing its LSN, got %d bytes", len(data))
		}
		m.LSN = pq.LSN(binary.BigEndian.Uint64(data[skipByte:]))
		skipByte += 8
	}

	relIdx := bytes.IndexByte(data[skipByte:], 0)
	if relIdx < 0 {
		return errors.Newf("logical decoding message prefix is not null terminated, remaining %d", len(data)-skipByte)
	}
	nullIdx := skipByte + relIdx

	m.Prefix = string(data[skipByte:nullIdx])
	skipByte = nullIdx + 1

	if len(data) < skipByte+4 {
		return errors.Newf("logical decoding message content length out of bounds, remaining %d", len(data)-skipByte)
	}

	contentLength := int(binary.BigEndian.Uint32(data[skipByte:]))
	skipByte += 4

	if len(data) < skipByte+contentLength {
		return errors.Newf("logical decoding message content length out of bounds, content %d, remaining %d", contentLength, len(data)-skipByte)
	}

	m.Content = data[skipByte : skipByte+contentLength]

	return nil
}
