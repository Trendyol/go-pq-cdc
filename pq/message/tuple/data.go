package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

type Data struct {
	Columns      DataColumns
	SkipByte     int
	ColumnNumber uint16
}

type DataColumns []*DataColumn

type DataColumn struct {
	Data     []byte
	Length   uint32
	DataType uint8
}

type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

func NewData(data []byte, tupleDataType uint8, skipByteLength int) (*Data, error) {
	if skipByteLength >= len(data) {
		return nil, errors.Newf("tuple data too short: want byte %d, have %d", skipByteLength, len(data))
	}
	if data[skipByteLength] != tupleDataType {
		return nil, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	d := &Data{}
	if err := d.Decode(data, skipByteLength); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *Data) Decode(data []byte, skipByteLength int) error {
	if len(data)-skipByteLength < 2 {
		return errors.Newf("tuple data too short for column count: want 2 bytes, have %d", len(data)-skipByteLength)
	}
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	for range d.ColumnNumber {
		if skipByteLength >= len(data) {
			return errors.Newf("tuple data too short for column type at byte %d, have %d", skipByteLength, len(data))
		}
		col := new(DataColumn)
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			if len(data)-skipByteLength < 4 {
				return errors.Newf("tuple data too short for column length: want 4 bytes, have %d", len(data)-skipByteLength)
			}
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			if uint64(col.Length) > uint64(len(data)-skipByteLength) {
				return errors.Newf("tuple column length %d exceeds remaining %d bytes", col.Length, len(data)-skipByteLength)
			}
			col.Data = make([]byte, col.Length)
			copy(col.Data, data[skipByteLength:])

			skipByteLength += int(col.Length)
		}

		d.Columns = append(d.Columns, col)
	}
	d.SkipByte = skipByteLength
	return nil
}

func (d *Data) DecodeWithColumn(columns []RelationColumn) (map[string]any, error) {
	if len(d.Columns) > len(columns) {
		return nil, errors.Newf("tuple has %d columns but relation defines %d", len(d.Columns), len(columns))
	}
	decoded := make(map[string]any, d.ColumnNumber)
	for idx, col := range d.Columns {
		relCol := columns[idx] //nolint:gosec // idx < len(d.Columns) <= len(columns), guaranteed by the guard above
		switch col.DataType {
		case DataTypeNull:
			decoded[relCol.Name] = nil
		case DataTypeText:
			val, err := decodeTextColumnData(col.Data, relCol.DataType)
			if err != nil {
				return nil, errors.Wrap(err, "decode column")
			}
			decoded[relCol.Name] = val
		}
	}

	return decoded, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
