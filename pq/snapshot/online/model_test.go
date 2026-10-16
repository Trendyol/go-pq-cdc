package online

import (
	"errors"
	"github.com/Trendyol/go-pq-cdc/pq"
	"testing"
)

func TestOnlineModel(t *testing.T) {
	t.Run("durable-two-window-restarts", testOnlineDurableSchedules)
	t.Run("old-generation-and-stale-attempt-controls", testOnlineModelReplayFencing)
	// Exhaust all subsets/orders of colliding CDC across three keys, including
	// an all-conflict page. A key in a different table must not remove a READ.
	table := Table{"public", "a"}
	other := Table{"public", "b"}
	for mask := 0; mask < 8; mask++ {
		rows := []SelectedRow{{Key: -1, Image: map[string]any{"id": int64(-1)}}, {Key: 0, Image: map[string]any{"id": int64(0)}}, {Key: 1, Image: map[string]any{"id": int64(1)}}}
		w := NewReadWindow(table, rows)
		w.Conflict(other, -1)
		for key := int64(-1); key <= 1; key++ {
			if mask&(1<<uint(key+1)) != 0 {
				w.Conflict(table, key)
			}
		}
		events := w.Reads("job", "attempt", pq.LSN(200))
		expected := 3
		for bit := 0; bit < 3; bit++ {
			if mask&(1<<bit) != 0 {
				expected--
			}
		}
		if len(events) != expected {
			t.Fatalf("mask %d: %d READs", mask, len(events))
		}
		for _, e := range events {
			if e.CommitLSN != 0 || e.Order.Phase != 1 || e.Order.EndLSN != 200 {
				t.Fatal("wrong HIGH boundary")
			}
		}
		// Output boundary, not arrival/receive position, must fence post-HIGH.
		for _, e := range events {
			if e.Order.Compare(OrderToken{EndLSN: 201}) >= 0 {
				t.Fatal("post-HIGH overtook READ")
			}
		}
		if last := w.LastKey(); last == nil || *last != 1 {
			t.Fatal("conflicts changed selected cursor")
		}
	}
}

func testOnlineModelReplayFencing(t *testing.T) {
	jobID, _ := NewID()
	connector, _ := NewID()
	oldAttempt, _ := NewID()
	newAttempt, _ := NewID()
	oldID, _ := NewID()
	newID, _ := NewID()
	job := Job{ID: jobID, ConnectorID: connector, Generation: 2}
	expected := Marker{ConnectorID: connector, JobID: jobID, Generation: 2, AttemptID: newAttempt, Phase: Low, ID: newID}
	old := Marker{ConnectorID: connector, JobID: jobID, Generation: 1, AttemptID: oldAttempt, Phase: High, ID: oldID}
	if found, err := matchMarker(job, old, &expected, 1); found || err != nil {
		t.Fatalf("old-generation control must be ignored, not satisfy fresh LOW: found=%v err=%v", found, err)
	}
	stale := old
	stale.Generation = 2
	if _, err := matchMarker(job, stale, &expected, 1); !errors.Is(err, ErrConditional) {
		t.Fatalf("same-generation stale attempt accepted: %v", err)
	}
	if found, err := matchMarker(job, expected, &expected, 1); !found || err != nil {
		t.Fatalf("fresh LOW rejected: %v", err)
	}
	high := expected
	high.Phase = High
	high.ID = oldID
	if _, err := matchMarker(job, expected, &high, 1); !errors.Is(err, ErrConditional) {
		t.Fatal("duplicate LOW accepted while waiting HIGH")
	}
	if _, err := matchMarker(job, high, nil, 1); !errors.Is(err, ErrConditional) {
		t.Fatal("late current-generation HIGH accepted after completion")
	}
	if _, err := matchMarker(job, high, &high, 2); !errors.Is(err, ErrConditional) {
		t.Fatal("mixed business/control HIGH accepted")
	}
	future := expected
	future.Generation++
	if _, err := matchMarker(job, future, &expected, 1); !errors.Is(err, ErrConditional) {
		t.Fatal("future generation accepted")
	}
	oldRead := NewReadWindow(Table{"public", "a"}, []SelectedRow{{Key: 1, Image: map[string]any{"id": int64(1)}}}).Reads(jobID, oldAttempt, 200)[0]
	newRead := NewReadWindow(Table{"public", "a"}, []SelectedRow{{Key: 1, Image: map[string]any{"id": int64(1)}}}).Reads(jobID, newAttempt, 300)[0]
	if oldRead.ID == newRead.ID || oldRead.Order.Compare(newRead.Order) >= 0 {
		t.Fatal("new attempt must replace old READ identity/order")
	}
}
