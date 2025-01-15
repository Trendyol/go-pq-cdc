package format

import (
	"testing"

	"github.com/vskurikhin/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
)

func TestRelation_New(t *testing.T) {
	data := []byte{82, 0, 0, 64, 6, 112, 117, 98, 108, 105, 99, 0, 116, 0, 100, 0, 2, 1, 105, 100, 0, 0, 0, 0, 23, 255, 255, 255, 255, 0, 110, 97, 109, 101, 0, 0, 0, 0, 25, 255, 255, 255, 255}

	rel, err := NewRelation(data, false)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Relation{
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
	}

	assert.Equal(t, expected, rel)
}
