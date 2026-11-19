package online

import (
	"reflect"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

func TestBusinessEventPreservesRealDecodedMetadata(t *testing.T) {
	table := Table{Schema: "public", Name: "items"}
	schemas := []TableSchema{{Table: table, Key: "id", Columns: []Column{{Name: "id", NotNull: true}, {Name: "value"}}}}
	old := map[string]any{"id": int32(7), "value": "before"}
	image := map[string]any{"id": int32(7), "value": "after"}
	clock := time.Date(2026, 11, 13, 9, 10, 11, 123000, time.UTC)
	messages := []any{
		&format.Insert{TableNamespace: "public", TableName: "items", Decoded: image, LSN: 50, MessageTime: clock},
		&format.Update{TableNamespace: "public", TableName: "items", NewDecoded: image, OldDecoded: old, LSN: 51, MessageTime: clock},
		&format.Delete{TableNamespace: "public", TableName: "items", OldDecoded: old, LSN: 52, MessageTime: clock},
	}
	for i, message := range messages {
		event, err := BusinessEvent(replication.OnlineChange{Message: message, Ordinal: uint32(i)}, &replication.OnlineCommittedEnvelope{XID: 99, CommitLSN: 100, EndLSN: 110}, schemas, "binding")
		require.NoError(t, err)
		// Reflection lets RED fail on lost metadata, not a missing-field compiler error.
		value := reflect.ValueOf(event)
		for name, expected := range map[string]any{"LSN": pq.LSN(50 + i), "TransactionID": uint32(99), "EventTime": clock} {
			field := value.FieldByName(name)
			require.True(t, field.IsValid(), "BusinessEvent discarded decoded %s", name)
			require.Equal(t, expected, field.Interface())
		}
		before := value.FieldByName("Before")
		require.True(t, before.IsValid(), "BusinessEvent discarded real old image")
		if i == 0 {
			require.Nil(t, before.Interface())
		} else {
			require.Equal(t, old, before.Interface())
			require.Equal(t, reflect.ValueOf(old).Pointer(), before.Pointer(), "metadata must reference already-budgeted old map, not duplicate it")
		}
	}
}

func TestReadEventMetadataIsUnavailableNotFabricated(t *testing.T) {
	events := NewReadWindow(Table{"public", "items"}, []SelectedRow{{Key: 7, Image: map[string]any{"id": int32(7)}}}).Reads("job", "attempt", 110)
	require.Len(t, events, 1)
	value := reflect.ValueOf(events[0])
	field := value.FieldByName("EventTime")
	require.True(t, field.IsValid(), "READ must expose unavailable timestamp explicitly")
	require.True(t, field.Interface().(time.Time).IsZero())
	require.Zero(t, value.FieldByName("TransactionID").Interface())
	require.Nil(t, value.FieldByName("Before").Interface())
	require.Equal(t, pq.LSN(0), value.FieldByName("LSN").Interface(), "READ has no decoded WALStart; adapter uses HIGH explicitly")
}
