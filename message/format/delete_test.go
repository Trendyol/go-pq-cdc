package format

import (
	"github.com/3n0ugh/dcpg/message/tuple"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDelete_New(t *testing.T) {
	data := []byte{68, 0, 0, 64, 6, 79, 0, 2, 116, 0, 0, 0, 3, 54, 52, 53, 116, 0, 0, 0, 3, 102, 111, 111}

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

	msg, err := NewDelete(data, false, rel)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Delete{
		OID:          16390,
		XID:          0,
		OldTupleType: 79,
		OldTupleData: &tuple.Data{
			ColumnNumber: 2,
			Columns: tuple.DataColumns{
				{
					DataType: 116,
					Length:   3,
					Data:     []byte("645"),
				},
				{
					DataType: 116,
					Length:   3,
					Data:     []byte("foo"),
				},
			},
			SkipByte: 24,
		},
		OldDecoded: map[string]any{
			"id":   int32(645),
			"name": "foo",
		},
	}

	assert.Equal(t, expected, msg)
}
