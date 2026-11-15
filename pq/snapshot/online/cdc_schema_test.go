package online

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

func TestCDCRelationSchemaRestrictions(t *testing.T) {
	initial := []TableSchema{{Table: Table{"public", "items"}, OID: 7, Key: "id", Columns: []Column{{Name: "id", OID: 20, NotNull: true}}}}
	valid := func() *format.Relation {
		return &format.Relation{Namespace: "public", Name: "items", OID: 7, ReplicaID: 'f', Columns: []tuple.RelationColumn{{Name: "id", DataType: 20}, {Name: "label", DataType: 25}}}
	}
	s, err := CDCRelationSchema(replication.OnlineChange{Relation: valid()}, initial)
	require.NoError(t, err)
	require.Len(t, s.Columns, 2)
	require.Equal(t, "id", s.Key)
	require.Len(t, initial[0].Columns, 1, "startup schema must remain immutable")
	for _, mutate := range []func(*format.Relation){
		func(r *format.Relation) { r.ReplicaID = 'd' },
		func(r *format.Relation) { r.Columns[0].Name = "other" },
		func(r *format.Relation) { r.Columns[0].DataType = 25 },
		func(r *format.Relation) { r.Columns[1].DataType = 1700 },
		func(r *format.Relation) { r.OID = 8 },
	} {
		r := valid()
		mutate(r)
		_, err := CDCRelationSchema(replication.OnlineChange{Relation: r}, initial)
		require.ErrorIs(t, err, ErrSchema)
	}
	_, err = CDCRelationSchema(replication.OnlineChange{}, initial)
	require.ErrorIs(t, err, ErrSchema)
}
