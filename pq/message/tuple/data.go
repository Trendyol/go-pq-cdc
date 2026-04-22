// Package tuple provides decoding for PostgreSQL logical replication tuple data.
package tuple

import (
	"encoding/binary"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Tuple column data type markers.
const (
	DataTypeNull   = uint8('n')
	DataTypeToast  = uint8('u')
	DataTypeText   = uint8('t')
	DataTypeBinary = uint8('b')
)

var typeMap = pgtype.NewMap()

// Data holds decoded tuple column data from a replication message.
type Data struct {
	Columns      DataColumns
	SkipByte     int
	ColumnNumber uint16
}

// DataColumns is a slice of decoded tuple columns.
type DataColumns []*DataColumn

// DataColumn represents a single column value in a tuple.
type DataColumn struct {
	Data     []byte
	Length   uint32
	DataType uint8
}

// RelationColumn describes a column in a relation message.
type RelationColumn struct {
	Name         string
	DataType     uint32
	TypeModifier uint32
	Flags        uint8
}

// NewData parses raw bytes into tuple Data starting at the given offset.
func NewData(data []byte, tupleDataType uint8, skipByteLength int) (*Data, error) {
	if data[skipByteLength] != tupleDataType {
		return nil, errors.New("invalid tuple data type: " + string(data[skipByteLength]))
	}
	skipByteLength++

	d := &Data{}
	d.Decode(data, skipByteLength)

	return d, nil
}

// Decode reads column data from raw bytes starting at the given offset.
func (d *Data) Decode(data []byte, skipByteLength int) {
	d.ColumnNumber = binary.BigEndian.Uint16(data[skipByteLength:])
	skipByteLength += 2

	for range d.ColumnNumber {
		col := new(DataColumn)
		col.DataType = data[skipByteLength]
		skipByteLength++

		switch col.DataType {
		case DataTypeNull, DataTypeToast:
		case DataTypeText, DataTypeBinary:
			col.Length = binary.BigEndian.Uint32(data[skipByteLength:])
			skipByteLength += 4

			col.Data = make([]byte, int(col.Length))
			copy(col.Data, data[skipByteLength:])

			skipByteLength += int(col.Length)
		}

		d.Columns = append(d.Columns, col)
	}
	d.SkipByte = skipByteLength
}

// DecodeWithColumn decodes tuple columns into a name-value map using the relation schema.
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
