package format

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
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

// unchangedToastRelation mirrors the table in example/toast-unchanged-column: id is the key.
func unchangedToastRelation() map[uint32]*Relation {
	return map[uint32]*Relation{
		0x4001: {
			OID:           0x4001,
			Namespace:     "public",
			Name:          "docs",
			ColumnNumbers: 3,
			Columns: []tuple.RelationColumn{
				{Flags: 1, Name: "id", DataType: 23, TypeModifier: 4294967295},
				{Flags: 0, Name: "title", DataType: 25, TypeModifier: 4294967295},
				{Flags: 0, Name: "payload", DataType: 25, TypeModifier: 4294967295},
			},
		},
	}
}

func TestUpdate_UnchangedToast_KeyOldTupleDoesNotFillNonKeyColumn(t *testing.T) {
	// 'K' old tuple (non-key columns are 'n'), new tuple has unchanged TOAST 'u' payload.
	data := []byte{
		0x55, 0x00, 0x00, 0x40, 0x01, 0x4b, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x33, 0x6e, 0x6e,
		0x4e, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x34, 0x74, 0x00, 0x00, 0x00, 0x01, 0x62, 0x75,
	}

	msg, err := NewUpdate(data, false, unchangedToastRelation(), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]any{"id": int32(4), "title": "b"}, msg.NewDecoded)
	assert.Equal(t, map[string]any{"id": int32(3), "title": nil, "payload": nil}, msg.OldDecoded)
	assert.Equal(t, []string{"payload"}, msg.UnchangedToastColumns)
}

func TestUpdate_UnchangedToast_FullOldTupleFillsColumn(t *testing.T) {
	// Same layout as above but 'O' old tuple (REPLICA IDENTITY FULL) with a real payload value.
	data := []byte{
		0x55, 0x00, 0x00, 0x40, 0x01, 0x4f, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x33, 0x74, 0x00,
		0x00, 0x00, 0x01, 0x61, 0x74, 0x00, 0x00, 0x00, 0x03, 0x6f, 0x6c, 0x64,
		0x4e, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x34, 0x74, 0x00, 0x00, 0x00, 0x01, 0x62, 0x75,
	}

	msg, err := NewUpdate(data, false, unchangedToastRelation(), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]any{"id": int32(4), "title": "b", "payload": "old"}, msg.NewDecoded)
	assert.Equal(t, map[string]any{"id": int32(3), "title": "a", "payload": "old"}, msg.OldDecoded)
	assert.Nil(t, msg.UnchangedToastColumns)
}

func TestUpdate_UnchangedToast_NoOldTupleListsColumn(t *testing.T) {
	// 'N' only: no old tuple, so the unchanged TOAST payload cannot be filled.
	data := []byte{
		0x55, 0x00, 0x00, 0x40, 0x01, 0x4e, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x34, 0x74, 0x00,
		0x00, 0x00, 0x01, 0x62, 0x75,
	}

	msg, err := NewUpdate(data, false, unchangedToastRelation(), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]any{"id": int32(4), "title": "b"}, msg.NewDecoded)
	assert.Nil(t, msg.OldDecoded)
	assert.Equal(t, []string{"payload"}, msg.UnchangedToastColumns)
}

func TestUpdate_UnchangedToast_KeyColumnFilledFromKeyOldTuple(t *testing.T) {
	// 'K' old tuple carries the key id='3'. New tuple sends the key column as unchanged TOAST 'u'
	// (not realistic for an int key, but it exercises the key-column branch): id must be copied from the K tuple.
	data := []byte{
		0x55, 0x00, 0x00, 0x40, 0x01, 0x4b, 0x00, 0x03, 0x74, 0x00, 0x00, 0x00, 0x01, 0x33, 0x6e, 0x6e,
		0x4e, 0x00, 0x03, 0x75, 0x74, 0x00, 0x00, 0x00, 0x01, 0x62, 0x6e,
	}

	msg, err := NewUpdate(data, false, unchangedToastRelation(), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, map[string]any{"id": int32(3), "title": "b", "payload": nil}, msg.NewDecoded)
	assert.Nil(t, msg.UnchangedToastColumns)
}
