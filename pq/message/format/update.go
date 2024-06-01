package format

import (
	"encoding/binary"
	"github.com/3n0ugh/dcpg/pq/message/tuple"
	"github.com/go-playground/errors"
)

const (
	UpdateTupleTypeKey = 'K'
	UpdateTupleTypeOld = 'O'
	UpdateTupleTypeNew = 'N'
)

type Update struct {
	OID          uint32
	XID          uint32
	NewTupleData *tuple.Data
	NewDecoded   map[string]any
	OldTupleType uint8
	OldTupleData *tuple.Data
	OldDecoded   map[string]any
}

func NewUpdate(data []byte, streamedTransaction bool, relation map[uint32]*Relation) (*Update, error) {
	msg := &Update{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	var err error

	if msg.OldTupleData != nil {
		msg.OldDecoded, err = msg.OldTupleData.DecodeWithColumn(rel.Columns)
		if err != nil {
			return nil, err
		}
	}

	msg.NewDecoded, err = msg.NewTupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Update) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 11 {
			return errors.Newf("streamed transaction update message length must be at least 11 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 7 {
		return errors.Newf("update message length must be at least 7 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	m.OldTupleType = data[skipByte]

	var err error

	switch m.OldTupleType {
	case UpdateTupleTypeKey, UpdateTupleTypeOld:
		m.OldTupleData, err = tuple.NewData(data, m.OldTupleType, skipByte)
		if err != nil {
			return errors.Wrap(err, "update message old tuple data")
		}
		skipByte += m.OldTupleData.SkipByte + 1
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, err = tuple.NewData(data, UpdateTupleTypeNew, m.OldTupleData.SkipByte)
		if err != nil {
			return errors.Wrap(err, "update message new tuple data")
		}
	default:
		return errors.New("update message undefined tuple type")
	}

	return nil
}