package format

import (
	"bytes"
	"encoding/binary"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/go-playground/errors"
)

type Relation struct {
	Namespace     string
	Name          string
	Columns       []tuple.RelationColumn
	OID           uint32
	XID           uint32
	ColumnNumbers uint16
	ReplicaID     uint8
}

func NewRelation(data []byte, streamedTransaction bool) (*Relation, error) {
	msg := &Relation{}
	if err := msg.decode(data, streamedTransaction); err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *Relation) decode(data []byte, streamedTransaction bool) error {
	skipByte := 1

	if streamedTransaction {
		if len(data) < 12 {
			return errors.Newf("streamed transaction relation message length must be at least 12 byte, but got %d", len(data))
		}

		m.XID = binary.BigEndian.Uint32(data[skipByte:])
		skipByte += 4
	}

	if len(data) < 8 {
		return errors.Newf("relation message length must be at least 8 byte, but got %d", len(data))
	}

	m.OID = binary.BigEndian.Uint32(data[skipByte:])
	skipByte += 4

	var usedByteCount int
	m.Namespace, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return errors.New("relation message namespace decode error")
	}
	skipByte += usedByteCount

	m.Name, usedByteCount = decodeString(data[skipByte:])
	if usedByteCount < 0 {
		return errors.New("relation message name decode error")
	}
	skipByte += usedByteCount

	if skipByte >= len(data) {
		return errors.New("relation message missing replica identity")
	}
	m.ReplicaID = data[skipByte]
	skipByte++

	if len(data)-skipByte < 2 {
		return errors.New("relation message missing column count")
	}
	m.ColumnNumbers = binary.BigEndian.Uint16(data[skipByte:])
	skipByte += 2

	m.Columns = make([]tuple.RelationColumn, m.ColumnNumbers)
	for i := range m.Columns {
		col, used, err := decodeRelationColumn(data[skipByte:], i)
		if err != nil {
			return err
		}
		m.Columns[i] = col
		skipByte += used
	}

	return nil
}

// decodeRelationColumn decodes one column descriptor from the start of data and
// returns the column together with the number of bytes it consumed.
func decodeRelationColumn(data []byte, i int) (tuple.RelationColumn, int, error) {
	var col tuple.RelationColumn

	if len(data) == 0 {
		return col, 0, errors.Newf("relation message columns[%d] missing flags", i)
	}
	col.Flags = data[0]
	skip := 1

	name, used := decodeString(data[skip:])
	if used < 0 {
		return col, 0, errors.Newf("relation message columns[%d].name decode error", i)
	}
	col.Name = name
	skip += used

	if len(data)-skip < 8 {
		return col, 0, errors.Newf("relation message columns[%d] missing type info", i)
	}
	col.DataType = binary.BigEndian.Uint32(data[skip:])
	skip += 4
	col.TypeModifier = binary.BigEndian.Uint32(data[skip:])
	skip += 4

	return col, skip, nil
}

func decodeString(data []byte) (string, int) {
	end := bytes.IndexByte(data, byte(0))
	if end == -1 {
		return "", -1
	}

	return string(data[:end]), end + 1
}
