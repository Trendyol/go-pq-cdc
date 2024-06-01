package format

import (
	"github.com/3n0ugh/dcpg/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsert_New(t *testing.T) {
	data := []byte{73, 0, 0, 64, 6, 78, 0, 2, 116, 0, 0, 0, 3, 54, 48, 53, 116, 0, 0, 0, 3, 102, 111, 111}

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

	msg, err := NewInsert(data, false, rel)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Insert{
		OID: 16390,
		XID: 0,
		TupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{DataType: 116, Length: 3, Data: []byte{'6', '0', '5'}},
				{DataType: 116, Length: 3, Data: []byte{'f', 'o', 'o'}},
			},
			SkipByte: 24,
		},
		Decoded: map[string]any{"id": int32(605), "name": "foo"},
	}

	assert.EqualValues(t, expected, msg)
}