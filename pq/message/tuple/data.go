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
		return nil, errors.Newf("tuple data length must be at least %d byte, but got %d", skipByteLength+1, len(data))
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
	if len(data) < skipByteLength+2 {
		return errors.Newf("tuple data length must be at least %d byte, but got %d", skipByteLength+2, len(data))
	}
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	for range d.ColumnNumber {
		if skipByteLength >= len(data) {
			return errors.Newf("tuple data length must be at least %d byte, but got %d", skipByteLength+1, len(data))
		}
		col := new(DataColumn)
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			if len(data) < skipByteLength+4 {
				return errors.Newf("tuple data length must be at least %d byte, but got %d", skipByteLength+4, len(data))
			}
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			if len(data) < skipByteLength+int(col.Length) {
				return errors.Newf("tuple data length must be at least %d byte, but got %d", skipByteLength+int(col.Length), len(data))
			}
			col.Data = make([]byte, int(col.Length))
			copy(col.Data, data[skipByteLength:])

			skipByteLength += int(col.Length)
		}

		d.Columns = append(d.Columns, col)
	}
	d.SkipByte = skipByteLength
	return nil
}

func (d *Data) DecodeWithColumn(columns []RelationColumn) (map[string]any, error) {
	decoded := make(map[string]any, d.ColumnNumber)
	for idx, col := range d.Columns {
		colName := columns[idx].Name
		switch col.DataType {
		case DataTypeNull:
			decoded[colName] = nil
		case DataTypeText:
			val, err := decodeTextColumnData(col.Data, columns[idx].DataType)
			if err != nil {
				return nil, errors.Wrap(err, "decode column")
			}
			decoded[colName] = val
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
