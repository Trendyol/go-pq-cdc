package format

import (
	"testing"
	"time"

	"github.com/vskurikhin/go-pq-cdc/pq/message/tuple"
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
