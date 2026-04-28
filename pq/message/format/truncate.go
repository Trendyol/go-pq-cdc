package format

import (
	"encoding/binary"
	"time"

	"github.com/go-playground/errors"
)

const (
	TruncateOptionCascade         = 1
	TruncateOptionRestartIdentity = 2
)

type Truncate struct {
	MessageTime     time.Time
	Relations       []*Relation
	RelationOIDs    []uint32
	XID             uint32
	Options         uint8
	Cascade         bool
	RestartIdentity bool
}

func NewTruncate(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Truncate, error) {
	msg := &Truncate{
		MessageTime: serverTime,
	}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	msg.Relations = make([]*Relation, len(msg.RelationOIDs))
	for i, oid := range msg.RelationOIDs {
		rel, ok := relation[oid]
		if !ok {
			return nil, errors.New("relation not found")
		}
		msg.Relations[i] = rel
	}

	return msg, nil
}

func (m *Truncate) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 10 {
			return errors.Newf("streamed transaction truncate message length must be at least 10 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < skipByte+5 {
		return errors.Newf("truncate message length must be at least %d byte, but got %d", skipByte+5, len(data))
	}

	relationCount := int(binary.BigEndian.Uint32(data[skipByte:]))
	skipByte += 4
	m.Options = data[skipByte]
	skipByte++

	if len(data) < skipByte+(relationCount*4) {
		return errors.Newf("truncate message relation IDs length must be %d byte, but got %d", relationCount*4, len(data)-skipByte)
	}

	m.Cascade = m.Options&TruncateOptionCascade != 0
	m.RestartIdentity = m.Options&TruncateOptionRestartIdentity != 0
	m.RelationOIDs = make([]uint32, relationCount)
	for i := range m.RelationOIDs {
		m.RelationOIDs[i] = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	return nil
}
