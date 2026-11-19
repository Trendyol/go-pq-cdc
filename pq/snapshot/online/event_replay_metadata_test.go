package online

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOnlineEventReplayJSONExcludesOnlyVolatileSenderClock(t *testing.T) {
	event := Event{
		ID: "cdc:stable", Table: Table{"public", "items"}, Key: 7, Operation: Update,
		Image: map[string]any{"id": int32(7), "value": "after"}, Before: map[string]any{"id": int32(7), "value": "before"},
		LSN: 50, TransactionID: 99, CommitLSN: 100, Order: OrderToken{EndLSN: 110, Ordinal: 1},
		EventTime: time.Date(2026, 11, 13, 9, 10, 11, 0, time.UTC),
	}
	replay := event
	replay.EventTime = event.EventTime.Add(time.Hour)
	first, err := json.Marshal(event)
	require.NoError(t, err)
	second, err := json.Marshal(replay)
	require.NoError(t, err)
	require.Equal(t, first, second, "source sender observation time changes on replay, not stable event content")
	require.Equal(t, sha256.Sum256(first), sha256.Sum256(second), "strict identity-reuse digest must remain stable")
	require.NotEqual(t, event.EventTime, replay.EventTime, "actual Go metadata remains available")
	for name, mutate := range map[string]func(*Event){
		"ID":            func(e *Event) { e.ID = "cdc:other" },
		"Table":         func(e *Event) { e.Table.Name = "other" },
		"Key":           func(e *Event) { e.Key = 8 },
		"Operation":     func(e *Event) { e.Operation = Insert },
		"Image":         func(e *Event) { e.Image = map[string]any{"id": int32(7), "value": "changed"} },
		"Before":        func(e *Event) { e.Before = map[string]any{"id": int32(7), "value": "changed"} },
		"LSN":           func(e *Event) { e.LSN = 51 },
		"TransactionID": func(e *Event) { e.TransactionID = 100 },
		"CommitLSN":     func(e *Event) { e.CommitLSN = 101 },
		"Order":         func(e *Event) { e.Order.Ordinal = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := event
			mutate(&changed)
			data, err := json.Marshal(changed)
			require.NoError(t, err)
			require.NotEqual(t, first, data, "stable source metadata/content must still participate in identity-reuse checks")
			require.NotEqual(t, sha256.Sum256(first), sha256.Sum256(data))
		})
	}
}
