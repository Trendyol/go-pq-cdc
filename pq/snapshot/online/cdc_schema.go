package online

import (
	"fmt"

	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

// CDCRelationSchema follows the descriptor used for this row, rather than a
// newer reader-cache entry or live catalog state. The startup key remains the
// event key; primary-key-only DDL is not detected in CDC-only mode.
func CDCRelationSchema(change replication.OnlineChange, initial []TableSchema) (TableSchema, error) {
	rel := change.Relation
	if rel == nil || rel.ReplicaID != 'f' {
		return TableSchema{}, fmt.Errorf("%w: CDC requires FULL relation metadata", ErrSchema)
	}
	for _, original := range initial {
		if original.Table != (Table{Schema: rel.Namespace, Name: rel.Name}) || original.OID != rel.OID {
			continue
		}
		schema := original
		schema.Columns = make([]Column, 0, len(rel.Columns))
		keyFound := false
		for _, col := range rel.Columns {
			if !tuple.OnlineBusinessOID(col.DataType) {
				return TableSchema{}, fmt.Errorf("%w: unsupported CDC column OID %d", ErrSchema, col.DataType)
			}
			isKey := col.Name == original.Key
			if isKey {
				if col.DataType != 20 && col.DataType != 21 && col.DataType != 23 {
					return TableSchema{}, fmt.Errorf("%w: non-integer CDC key", ErrSchema)
				}
				keyFound = true
			}
			// Relation does not expose NOT NULL constraints. Require a nonnull event
			// key; other nullability changes follow the incoming complete row image.
			schema.Columns = append(schema.Columns, Column{Name: col.Name, OID: col.DataType, Modifier: int32(col.TypeModifier), NotNull: isKey})
		}
		if !keyFound {
			return TableSchema{}, fmt.Errorf("%w: startup key missing from CDC relation", ErrSchema)
		}
		return schema, nil
	}
	return TableSchema{}, fmt.Errorf("%w: uncataloged CDC relation", ErrSchema)
}
