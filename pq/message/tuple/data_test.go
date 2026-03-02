package tuple

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewData(t *testing.T) {
	tupleDataType := uint8('D')

	// Payload construction
	buf := []byte{}
	// Skip bytes (simulated garbage or other headers)
	skipBytes := []byte{0x00}
	buf = append(buf, skipBytes...)

	// Tuple Data Type
	buf = append(buf, tupleDataType)

	// Column count (2)
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, 2)
	buf = append(buf, colCount...)

	// Col 1: Text 't'
	buf = append(buf, DataTypeText)
	// Col 1 Length: 5 ("value")
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 5)
	buf = append(buf, lenBuf...)
	// Col 1 Data
	buf = append(buf, []byte("value")...)

	// Col 2: Null 'n'
	buf = append(buf, DataTypeNull)

	t.Run("should parse valid data", func(t *testing.T) {
		d, err := NewData(buf, tupleDataType, len(skipBytes))
		require.NoError(t, err)
		assert.NotNil(t, d)
		assert.Equal(t, uint16(2), d.ColumnNumber)
		assert.Len(t, d.Columns, 2)

		assert.Equal(t, DataTypeText, d.Columns[0].DataType)
		assert.Equal(t, uint32(5), d.Columns[0].Length)
		assert.Equal(t, []byte("value"), d.Columns[0].Data)

		assert.Equal(t, DataTypeNull, d.Columns[1].DataType)
	})

	t.Run("should return error for invalid tuple data type", func(t *testing.T) {
		_, err := NewData(buf, 'X', len(skipBytes)) // Expecting 'X', but got 'D'
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid tuple data type: D")
	})
}

func TestData_DecodeWithColumn(t *testing.T) {
	tupleDataType := uint8('D')
	buf := []byte{}
	buf = append(buf, tupleDataType) // No skip bytes this time for simplicity

	// Column count (2)
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, 2)
	buf = append(buf, colCount...)

	// Col 1: Text 't' - Int value "123"
	buf = append(buf, DataTypeText)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, 3)
	buf = append(buf, lenBuf...)
	buf = append(buf, []byte("123")...)

	// Col 2: Null 'n'
	buf = append(buf, DataTypeNull)

	d, _ := NewData(buf, tupleDataType, 0)

	t.Run("should decode columns correctly", func(t *testing.T) {
		relationColumns := []RelationColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "description", DataType: pgtype.TextOID},
		}

		decoded, err := d.DecodeWithColumn(relationColumns)
		require.NoError(t, err)
		assert.NotNil(t, decoded)
		assert.Equal(t, int32(123), decoded["id"])
		assert.Nil(t, decoded["description"])
	})

	t.Run("should use string fallback for unknown types", func(t *testing.T) {
		relationColumns := []RelationColumn{
			{Name: "unknown_col", DataType: 999999}, // Unknown OID
			{Name: "description", DataType: pgtype.TextOID},
		}

		decoded, err := d.DecodeWithColumn(relationColumns)
		require.NoError(t, err)
		assert.Equal(t, "123", decoded["unknown_col"]) // Fallback to string
	})
}
