package format

import (
	"encoding/binary"
	"github.com/3n0ugh/dcpg/message/tuple"
	"github.com/go-playground/errors"
)

type Delete struct {
	OID          uint32
	XID          uint32
	OldTupleType uint8
	OldTupleData *tuple.Data
	OldDecoded   map[string]any
}

func NewDelete(data []byte, streamedTransaction bool, relation map[uint32]*Relation) (*Delete, error) {
	msg := &Delete{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	var err error

	msg.OldDecoded, err = msg.OldTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Delete) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction delete message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("delete message length must be at least 7 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
	if err != nil {
		return errors.Wrap(err, "delete message old tuple data")
	}
	skipByte += m.OldTupleData.SkipByte + 1

	return nil
}
