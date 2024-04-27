package format

import (
	"encoding/binary"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
	"gitlab.trendyol.com/pq-dcp/message/tuple"
)

const (
	InsertTupleDataType = 'N'
)

var typeMap = pgtype.NewMap()

type Insert struct {
	OID       uint32
	XID       uint32
	TupleData *tuple.Data
	Decoded   map[string]any
}

func NewInsert(data []byte, streamedTransaction bool, relation map[uint32]*Relation) (*Insert, error) {
	msg := &Insert{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	rel, ok := relation[msg.OID]
	if !ok {
		return nil, errors.New("relation not found")
	}

	msg.Decoded = make(map[string]any)

	var err error
	msg.Decoded, err = msg.TupleData.DecodeWithColumn(rel.Columns)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Insert) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 13 {
			return errors.Newf("streamed transaction insert message length must be at least 13 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 9 {
		return errors.Newf("insert message length must be at least 9 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var err error

	m.TupleData, err = tuple.NewData(data, InsertTupleDataType, skipByte)
	if err != nil {
		return errors.Wrap(err, "insert message")
	}

	return nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
