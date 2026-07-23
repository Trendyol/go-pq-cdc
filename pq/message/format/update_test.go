package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdate_New(t *testing.T) {
	data := []byte{85, 0, 0, 64, 6, 79, 0, 2, 116, 0, 0, 0, 2, 53, 51, 116, 0, 0, 0, 4, 98, 97, 114, 50, 78, 0, 2, 116, 0, 0, 0, 2, 53, 51, 116, 0, 0, 0, 4, 98, 97, 114, 53}

	rel := map[uint32]*Relation{
		16390: {
			OID:           16390,
			XID:           0,
			Namespace:     "public",
			Name:          "t",
			ReplicaID:     100,
			ColumnNumbers: 2,
			Columns: []tuple.RelationColumn{
				{
					Flags:        1,
					Name:         "id",
					DataType:     23,
					TypeModifier: 4294967295,
				},
				{
					Flags:        0,
					Name:         "name",
					DataType:     25,
					TypeModifier: 4294967295,
				},
			},
		},
	}

	now := time.Now()
	msg, err := NewUpdate(data, false, rel, now)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Update{
		OID: 16390,
		XID: 0,
		NewTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{
					DataType: 116,
					Length:   2,
					Data:     []byte("53"),
				},
				{
					DataType: 116,
					Length:   4,
					Data:     []byte("bar5"),
				},
			},
			SkipByte: 43,
		},
		NewDecoded: map[string]any{
			"id":   int32(53),
			"name": "bar5",
		},
		OldTupleType: 79,
		OldTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{
					DataType: 116,
					Length:   2,
					Data:     []byte("53"),
				},
				{
					DataType: 116,
					Length:   4,
					Data:     []byte("bar2"),
				},
			},
			SkipByte: 24,
		},
		OldDecoded: map[string]any{
			"id":   int32(53),
			"name": "bar2",
		},
		TableNamespace: "public",
		TableName:      "t",
		MessageTime:    now,
	}

	assert.Equal(t, expected, msg)
}

func toastTestRelation() map[uint32]*Relation {
	return map[uint32]*Relation{
		16390: {
			OID:           16390,
			Namespace:     "public",
			Name:          "t",
			ColumnNumbers: 2,
			Columns: []tuple.RelationColumn{
				{Flags: 1, Name: "id", DataType: 23, TypeModifier: 4294967295},
				{Flags: 0, Name: "name", DataType: 25, TypeModifier: 4294967295},
			},
		},
	}
}

// A key only old tuple carries fewer columns than the new tuple. When the new
// tuple has a toasted column at an index the old tuple does not have, the merge
// must skip it instead of indexing past the old columns, which panicked before
// the guard was added.
func TestUpdate_ToastColumnBeyondOldTupleIsSkipped(t *testing.T) {
	data := []byte{
		'U',
		0, 0, 64, 6, // OID 16390
		'K',  // key typed old tuple
		0, 1, // old tuple has a single column
		't', 0, 0, 0, 2, '5', '3',
		'N',
		0, 2, // new tuple declares two columns
		't', 0, 0, 0, 2, '5', '3',
		'u', // toast at index 1, beyond the old tuple
	}

	msg, err := NewUpdate(data, false, toastTestRelation(), time.Now())

	require.NoError(t, err)
	require.Len(t, msg.NewTupleData.Columns, 2)
	assert.Equal(t, tuple.DataTypeToast, msg.NewTupleData.Columns[1].DataType)
	assert.Equal(t, map[string]any{"id": int32(53)}, msg.NewDecoded)
	assert.Equal(t, map[string]any{"id": int32(53)}, msg.OldDecoded)
}

// When the old tuple does carry the column, a toasted new column must be replaced
// by the old value. This pins the merge behavior itself.
func TestUpdate_ToastColumnMergedFromOldTuple(t *testing.T) {
	data := []byte{
		'U',
		0, 0, 64, 6, // OID 16390
		'O',  // full old tuple
		0, 2, // old tuple has both columns
		't', 0, 0, 0, 2, '5', '3',
		't', 0, 0, 0, 4, 'b', 'a', 'r', '2',
		'N',
		0, 2,
		't', 0, 0, 0, 2, '5', '3',
		'u', // toast at index 1, old value must be substituted
	}

	msg, err := NewUpdate(data, false, toastTestRelation(), time.Now())

	require.NoError(t, err)
	require.Len(t, msg.NewTupleData.Columns, 2)
	assert.Equal(t, tuple.DataTypeText, msg.NewTupleData.Columns[1].DataType)
	assert.Equal(t, []byte("bar2"), msg.NewTupleData.Columns[1].Data)
	assert.Equal(t, map[string]any{"id": int32(53), "name": "bar2"}, msg.NewDecoded)
}

func TestUpdate_StreamedTransaction(t *testing.T) {
	t.Run("decodes update with xid prefix", func(t *testing.T) {
		data := []byte{
			'U',
			0, 0, 0, 99, // XID 99
			0, 0, 64, 6, // OID 16390
			'O',
			0, 2,
			't', 0, 0, 0, 2, '5', '3',
			't', 0, 0, 0, 4, 'b', 'a', 'r', '2',
			'N',
			0, 2,
			't', 0, 0, 0, 2, '5', '3',
			't', 0, 0, 0, 4, 'b', 'a', 'r', '5',
		}

		msg, err := NewUpdate(data, true, toastTestRelation(), time.Now())

		require.NoError(t, err)
		assert.Equal(t, uint32(99), msg.XID)
		assert.Equal(t, uint32(16390), msg.OID)
		assert.Equal(t, map[string]any{"id": int32(53), "name": "bar5"}, msg.NewDecoded)
		assert.Equal(t, map[string]any{"id": int32(53), "name": "bar2"}, msg.OldDecoded)
	})

	t.Run("returns error when streamed update is too short", func(t *testing.T) {
		msg, err := NewUpdate([]byte{'U', 0, 0, 0, 9}, true, toastTestRelation(), time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "streamed transaction update message length must be at least 11 byte, but got 5")
	})
}

func TestUpdate_DecodeGuards(t *testing.T) {
	t.Run("returns error when update is too short", func(t *testing.T) {
		msg, err := NewUpdate([]byte{'U', 0, 0}, false, toastTestRelation(), time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "update message length must be at least 7 byte, but got 3")
	})

	t.Run("returns error for undefined tuple type", func(t *testing.T) {
		msg, err := NewUpdate([]byte{'U', 0, 0, 64, 6, 'X', 0}, false, toastTestRelation(), time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "update message undefined tuple type")
	})

	t.Run("returns error when relation is unknown", func(t *testing.T) {
		data := []byte{
			'U',
			0, 0, 64, 6,
			'N',
			0, 1,
			't', 0, 0, 0, 2, '5', '3',
		}

		msg, err := NewUpdate(data, false, map[uint32]*Relation{}, time.Now())

		require.Error(t, err)
		assert.Nil(t, msg)
		assert.Contains(t, err.Error(), "relation not found")
	})
}
