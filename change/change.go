// Package change gives every sink one view of a captured row change and a set
// of target-independent steps (filters, column selection, renames) that run on it.
package change

import (
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

const defaultSchema = "public"

type Op string

const (
	Insert   Op = "INSERT"
	Update   Op = "UPDATE"
	Delete   Op = "DELETE"
	Snapshot Op = "SNAPSHOT"
)

// Change is a row change in one shape regardless of the operation.
// Old is nil for inserts and snapshot rows, New is nil for deletes.
type Change struct {
	EventTime time.Time
	Old       map[string]any
	New       map[string]any
	Op        Op
	Schema    string
	Table     string
	LSN       pq.LSN
	Xid       uint32
}

// FromMessage converts an insert, update, delete or snapshot message.
// It returns false for every other message type (truncate, logical messages, ...).
func FromMessage(msg any) (*Change, bool) {
	switch m := msg.(type) {
	case *format.Insert:
		return &Change{
			Op:        Insert,
			Schema:    m.TableNamespace,
			Table:     m.TableName,
			New:       m.Decoded,
			LSN:       m.LSN,
			EventTime: m.MessageTime,
		}, true
	case *format.Update:
		return &Change{
			Op:        Update,
			Schema:    m.TableNamespace,
			Table:     m.TableName,
			Old:       m.OldDecoded,
			New:       m.NewDecoded,
			LSN:       m.LSN,
			EventTime: m.MessageTime,
		}, true
	case *format.Delete:
		return &Change{
			Op:        Delete,
			Schema:    m.TableNamespace,
			Table:     m.TableName,
			Old:       m.OldDecoded,
			LSN:       m.LSN,
			EventTime: m.MessageTime,
		}, true
	case *format.Snapshot:
		if m.EventType != format.SnapshotEventTypeData {
			return nil, false
		}
		return &Change{
			Op:        Snapshot,
			Schema:    m.Schema,
			Table:     m.Table,
			New:       m.Data,
			LSN:       m.LSN,
			EventTime: m.ServerTime,
		}, true
	default:
		return nil, false
	}
}

// FromListener converts the message a listener receives and adds its transaction id.
func FromListener(ctx *replication.ListenerContext) (*Change, bool) {
	c, ok := FromMessage(ctx.Message)
	if !ok {
		return nil, false
	}
	c.Xid = ctx.Xid
	return c, true
}

// FullName returns the table as schema.table.
func (c *Change) FullName() string {
	return c.Schema + "." + c.Table
}

// Row returns the row a sink usually writes: the old row for deletes, the new row otherwise.
func (c *Change) Row() map[string]any {
	if c.Op == Delete {
		return c.Old
	}
	return c.New
}

// Clone copies the row maps so steps can change them without touching the source message.
// Column values are shared.
func (c *Change) Clone() *Change {
	clone := *c
	clone.Old = cloneRow(c.Old)
	clone.New = cloneRow(c.New)
	return &clone
}

func cloneRow(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	clone := make(map[string]any, len(row))
	for k, v := range row {
		clone[k] = v
	}
	return clone
}
