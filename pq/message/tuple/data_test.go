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

func TestNewData_TruncatedReturnsError(t *testing.T) {
	t.Run("text column missing length bytes", func(t *testing.T) {
		data := []byte{DataTypeText, 0, 1, DataTypeText}
		_, err := NewData(data, DataTypeText, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tuple data too short for column length")
	})

	t.Run("column length exceeds remaining bytes", func(t *testing.T) {
		data := []byte{DataTypeText, 0, 1, DataTypeText, 0, 0, 0, 10, 'a', 'b'}
		_, err := NewData(data, DataTypeText, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds remaining")
	})

	t.Run("column count exceeds available data", func(t *testing.T) {
		data := []byte{DataTypeText, 0, 2, DataTypeNull}
		_, err := NewData(data, DataTypeText, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tuple data too short for column type")
	})

	t.Run("skip byte past end of data", func(t *testing.T) {
		_, err := NewData([]byte{0x00}, DataTypeText, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tuple data too short: want byte")
	})
}

// Decode requires two bytes for the column count after the type byte. One byte
// is the boundary the guard exists for.
func TestData_DecodeColumnCountGuard(t *testing.T) {
	_, err := NewData([]byte{'D', 0x00}, 'D', 0)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "tuple data too short for column count: want 2 bytes, have 1")
}

// Exactly two bytes after the skip is the minimum valid payload: a zero column
// tuple. It must decode cleanly.
func TestData_DecodeZeroColumnsBoundary(t *testing.T) {
	d := &Data{}

	err := d.Decode([]byte{0, 0}, 0)

	require.NoError(t, err)
	assert.Equal(t, uint16(0), d.ColumnNumber)
	assert.Equal(t, 2, d.SkipByte)
	assert.Empty(t, d.Columns)
}

// A tuple with fewer columns than the relation defines is normal, for example a
// key only old tuple, and must keep decoding after the over wide guard was added.
func TestDecodeWithColumn_FewerTupleColumnsThanRelation(t *testing.T) {
	d := &Data{
		ColumnNumber: 1,
		Columns: DataColumns{
			{DataType: DataTypeText, Data: []byte("53"), Length: 2},
		},
	}

	decoded, err := d.DecodeWithColumn([]RelationColumn{
		{Name: "id", DataType: pgtype.Int4OID},
		{Name: "name", DataType: pgtype.TextOID},
	})

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": int32(53)}, decoded)
}

func TestDecodeWithColumn_TooFewRelationColumns(t *testing.T) {
	d := &Data{
		ColumnNumber: 2,
		Columns: DataColumns{
			{DataType: DataTypeNull},
			{DataType: DataTypeNull},
		},
	}
	_, err := d.DecodeWithColumn([]RelationColumn{{Name: "id"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "relation defines")
}
