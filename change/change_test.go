package change

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromMessage(t *testing.T) {
	const lsn pq.LSN = 0x16B374D848
	now := time.Now()
	oldRow := map[string]any{"id": int32(1), "name": "old"}
	newRow := map[string]any{"id": int32(1), "name": "new"}

	tests := []struct {
		message any
		want    *Change
		name    string
	}{
		{
			name:    "insert",
			message: &format.Insert{TableNamespace: "public", TableName: "users", Decoded: newRow, LSN: lsn, MessageTime: now},
			want:    &Change{Op: Insert, Schema: "public", Table: "users", New: newRow, LSN: lsn, EventTime: now},
		},
		{
			name:    "update",
			message: &format.Update{TableNamespace: "public", TableName: "users", OldDecoded: oldRow, NewDecoded: newRow, LSN: lsn, MessageTime: now},
			want:    &Change{Op: Update, Schema: "public", Table: "users", Old: oldRow, New: newRow, LSN: lsn, EventTime: now},
		},
		{
			name:    "delete",
			message: &format.Delete{TableNamespace: "public", TableName: "users", OldDecoded: oldRow, LSN: lsn, MessageTime: now},
			want:    &Change{Op: Delete, Schema: "public", Table: "users", Old: oldRow, LSN: lsn, EventTime: now},
		},
		{
			name:    "snapshot row",
			message: &format.Snapshot{EventType: format.SnapshotEventTypeData, Schema: "public", Table: "users", Data: newRow, LSN: lsn, ServerTime: now},
			want:    &Change{Op: Snapshot, Schema: "public", Table: "users", New: newRow, LSN: lsn, EventTime: now},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromMessage(tt.message)
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFromMessage_Unsupported(t *testing.T) {
	tests := []struct {
		message any
		name    string
	}{
		{name: "truncate", message: &format.Truncate{}},
		{name: "logical message", message: &format.LogicalDecodingMessage{}},
		{name: "snapshot begin", message: &format.Snapshot{EventType: format.SnapshotEventTypeBegin}},
		{name: "snapshot end", message: &format.Snapshot{EventType: format.SnapshotEventTypeEnd}},
		{name: "nil", message: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromMessage(tt.message)
			assert.False(t, ok)
			assert.Nil(t, got)
		})
	}
}

func TestFromListener_AddsXid(t *testing.T) {
	got, ok := FromListener(&replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "public", TableName: "users"},
		Xid:     42,
	})

	require.True(t, ok)
	assert.Equal(t, uint32(42), got.Xid)
}

func TestFromListener_Unsupported(t *testing.T) {
	got, ok := FromListener(&replication.ListenerContext{Message: &format.Truncate{}, Xid: 42})

	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestChange_FullName(t *testing.T) {
	c := &Change{Schema: "sales", Table: "orders"}

	assert.Equal(t, "sales.orders", c.FullName())
}

func TestChange_Row(t *testing.T) {
	oldRow := map[string]any{"id": 1, "name": "old"}
	newRow := map[string]any{"id": 1, "name": "new"}

	tests := []struct {
		want map[string]any
		name string
		op   Op
	}{
		{name: "insert", op: Insert, want: newRow},
		{name: "update", op: Update, want: newRow},
		{name: "delete", op: Delete, want: oldRow},
		{name: "snapshot", op: Snapshot, want: newRow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Change{Op: tt.op, Old: oldRow, New: newRow}
			assert.Equal(t, tt.want, c.Row())
		})
	}
}

func TestChange_CloneLeavesSourceUntouched(t *testing.T) {
	source := &Change{
		Op:  Update,
		Old: map[string]any{"id": 1, "name": "old"},
		New: map[string]any{"id": 1, "name": "new"},
	}

	clone := source.Clone()
	delete(clone.Old, "name")
	clone.New["name"] = "changed"
	clone.Table = "other"

	assert.Equal(t, map[string]any{"id": 1, "name": "old"}, source.Old)
	assert.Equal(t, map[string]any{"id": 1, "name": "new"}, source.New)
	assert.Empty(t, source.Table)
}

func TestChange_CloneKeepsNilRows(t *testing.T) {
	clone := (&Change{Op: Insert, New: map[string]any{"id": 1}}).Clone()

	assert.Nil(t, clone.Old)
	assert.Equal(t, map[string]any{"id": 1}, clone.New)
}
