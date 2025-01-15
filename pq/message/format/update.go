package format

import (
	"encoding/binary"
	"time"

	"github.com/vskurikhin/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

const (
	UpdateTupleTypeKey = 'K'
	UpdateTupleTypeOld = 'O'
	UpdateTupleTypeNew = 'N'
)

type Update struct {
	MessageTime    time.Time
	NewTupleData   *tuple.Data
	NewDecoded     map[string]any
	OldTupleData   *tuple.Data
	OldDecoded     map[string]any
	TableNamespace string
	TableName      string
	OID            uint32
	XID            uint32
	OldTupleType   uint8
}

func NewUpdate(data []byte, streamedTransaction bool, relation map[uint32]*Relation, serverTime time.Time) (*Update, error) {
	msg := &Update{
		MessageTime: serverTime,
	}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	msg.TableNamespace = rel.Namespace
	msg.TableName = rel.Name

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
		skipByte = m.OldTupleData.SkipByte
		fallthrough
	case UpdateTupleTypeNew:
		m.NewTupleData, err = tuple.NewData(data, UpdateTupleTypeNew, skipByte)
		if err != nil {
			return errors.Wrap(err, "update message new tuple data")
		}

		if m.OldTupleData != nil {
			for i, col := range m.NewTupleData.Columns {
				// because toasted columns not sent until the toasted column updated
				if col.DataType == tuple.DataTypeToast {
					m.NewTupleData.Columns[i] = m.OldTupleData.Columns[i]
				}
			}
		}
	default:
		return errors.New("update message undefined tuple type")
	}

	return nil
}
