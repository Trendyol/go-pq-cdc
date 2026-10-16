package online

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
)

type modelDisk struct {
	checkpoint, confirmed pq.LSN
	cursor                *int64
	generation            int64
	completed             bool
}
type modelTarget struct {
	ledger     map[string]Event
	projection map[int64]Event
}

func (s *modelTarget) apply(e Event, reply int) bool {
	if reply == 1 {
		return false
	} // rollback before durable target commit
	if old, ok := s.ledger[e.ID]; ok {
		if !reflect.DeepEqual(old, e) {
			panic("identity content changed on replay")
		}
	} else {
		s.ledger[e.ID] = e
		if old, ok := s.projection[e.Key]; !ok || old.Order.Compare(e.Order) < 0 {
			s.projection[e.Key] = e
		}
	}
	return reply != 2 // commit followed by lost response
}

// Explicit durable state survives restart; SELECT rows, received positions and
// active attempt state do not. Each seed has three keys, two windows and two
// two-row source transactions. Real Store/connector fault schedules complement
// this model; it exercises the production window/identity/order/control seams.
func testOnlineDurableSchedules(t *testing.T) {
	phases := []string{"LOW", "SELECT", "HIGH", "ONE_READ", "LAST_READ", "METADATA", "CONFIRMED"}
	for seed := int64(0); seed < 32; seed++ {
		for _, phase := range phases {
			t.Run(fmt.Sprintf("seed-%d/%s", seed, phase), func(t *testing.T) {
				random := rand.New(rand.NewSource(seed))
				table := Table{"public", "model"}
				jobID := "00000000-0000-4000-8000-000000000001"
				binding := "model-slot-binding"
				cdc := []Event{
					{Table: table, Key: -1, Operation: Update, Image: map[string]any{"id": int64(-1), "v": "u1"}, CommitLSN: 149, Order: OrderToken{EndLSN: 150, Ordinal: 0}},
					{Table: table, Key: 1, Operation: Delete, CommitLSN: 149, Order: OrderToken{EndLSN: 150, Ordinal: 1}},
					{Table: table, Key: 0, Operation: Update, Image: map[string]any{"id": int64(0), "v": "u2"}, CommitLSN: 249, Order: OrderToken{EndLSN: 250, Ordinal: 0}},
					{Table: table, Key: 1, Operation: Insert, Image: map[string]any{"id": int64(1), "v": "reinsert"}, CommitLSN: 249, Order: OrderToken{EndLSN: 250, Ordinal: 1}},
				}
				for i := range cdc {
					cdc[i].ID = CDCEventID(binding, cdc[i].Order.EndLSN, cdc[i].Order.Ordinal)
				}
				disk := modelDisk{checkpoint: 100, confirmed: 100, generation: 1}
				sink := modelTarget{ledger: map[string]Event{}, projection: map[int64]Event{}}
				window := NewReadWindow(table, []SelectedRow{{Key: -1, Image: map[string]any{"id": int64(-1), "v": "old"}}, {Key: 0, Image: map[string]any{"id": int64(0), "v": "old"}}})
				trace := []string{"durable LOW100; SELECT frozen page[-1,0]"}
				calls := 0
				failCall := random.Intn(5)
				reply := 1 + random.Intn(2)
				stopped := phase == "LOW" || phase == "SELECT"
				highObserved := false
				deliver := func(e Event) bool {
					calls++
					fault := 0
					if calls == failCall {
						fault = reply
					}
					ok := sink.apply(e, fault)
					trace = append(trace, fmt.Sprintf("deliver %s/%d reply=%d", e.Operation, e.Key, fault))
					return ok
				}
				if !stopped {
					// Earlier XID may be invisible while a later XID is independently visible.
					// It blocks HIGH/delivery/frontier, never inferred from the later XID.
					if seed%2 == 0 {
						if disk.checkpoint != 100 || disk.confirmed != 100 {
							t.Fatal(trace)
						}
						trace = append(trace, "earlier XID invisible; later XID visible; no frontier change; release earlier")
					}
					for _, e := range cdc[:2] {
						window.Conflict(e.Table, e.Key)
						if !deliver(e) {
							stopped = true
							break
						}
					}
					if !stopped {
						highObserved = true
						if phase == "HIGH" {
							stopped = true
						}
					}
				}
				oldReads := window.Reads(jobID, "attempt-generation-1", 200)
				if !stopped {
					for _, e := range oldReads {
						if !deliver(e) {
							stopped = true
							break
						}
					}
					if phase == "ONE_READ" || phase == "LAST_READ" {
						stopped = true
					}
				}
				if !stopped {
					last := window.LastKey()
					disk.cursor = last
					disk.checkpoint = 200
					disk.completed = true
					trace = append(trace, "atomic cursor0 + complete + checkpoint200 commit")
					if phase == "METADATA" {
						stopped = true
					} else {
						disk.confirmed = 200
					}
				}
				// Crash discards all volatile work/received positions, not committed target
				// effects or an already committed completion whose response was lost.
				if disk.confirmed > disk.checkpoint {
					t.Fatal("unsafe ACK", trace)
				}
				if disk.confirmed == 200 {
					for _, e := range append(cdc[:2:2], oldReads...) {
						if _, ok := sink.ledger[e.ID]; !ok {
							t.Fatal("acknowledged obligation missing", trace)
						}
					}
				}
				restart := disk.checkpoint
				disk.generation++
				oldControl := Marker{ConnectorID: "connector", JobID: jobID, Generation: 1, AttemptID: "attempt-generation-1", Phase: High, ID: "old-high"}
				fresh := Marker{ConnectorID: "connector", JobID: jobID, Generation: 2, AttemptID: "attempt-generation-2", Phase: Low, ID: "fresh-low"}
				if found, err := matchMarker(Job{ID: jobID, ConnectorID: "connector", Generation: disk.generation}, oldControl, &fresh, 1); found || err != nil {
					t.Fatal("old HIGH satisfied new LOW", trace)
				}
				// Catch up all source obligations newer than persisted checkpoint. Replay IDs
				// retain source binding/end/ordinal, regardless of attempt generation.
				for _, e := range cdc {
					if e.Order.EndLSN > restart {
						if !sink.apply(e, 0) {
							t.Fatal(trace)
						}
					}
				}
				disk.checkpoint = 500
				disk.confirmed = 500
				source := []SelectedRow{{Key: -1, Image: cdc[0].Image}, {Key: 0, Image: cdc[2].Image}, {Key: 1, Image: cdc[3].Image}}
				var page []SelectedRow
				if disk.cursor == nil {
					page = source[:2]
				} else {
					if *disk.cursor != 0 {
						t.Fatal("completed cursor lost", trace)
					}
					page = source[2:]
				}
				next := NewReadWindow(table, page)
				for _, e := range next.Reads(jobID, "attempt-generation-2", 600) {
					sink.apply(e, 0)
				}
				disk.cursor = next.LastKey()
				disk.checkpoint = 600
				disk.confirmed = 600
				if *disk.cursor == 0 {
					last := NewReadWindow(table, source[2:])
					for _, e := range last.Reads(jobID, "attempt-generation-2-window-2", 800) {
						sink.apply(e, 0)
					}
					disk.cursor = last.LastKey()
					disk.checkpoint = 800
					disk.confirmed = 800
				}
				// A genuinely constructed old READ can finish late. Version/tombstone order
				// protection must leave the newer projection unchanged.
				if highObserved {
					before := sink.projection[0]
					for _, e := range oldReads {
						sink.apply(e, 0)
					}
					if !reflect.DeepEqual(before, sink.projection[0]) {
						t.Fatal("late READ overwrote newer state", trace)
					}
				}
				for _, e := range cdc {
					if _, ok := sink.ledger[e.ID]; !ok {
						t.Fatal("missing intermediate CDC identity", trace)
					}
				}
				for _, row := range source {
					got := sink.projection[row.Key]
					if got.Operation == Delete || !reflect.DeepEqual(got.Image, row.Image) {
						t.Fatal("final exact state mismatch", trace)
					}
				}
				if *disk.cursor != 1 || disk.confirmed > disk.checkpoint {
					t.Fatal("finite progress/ACK invariant", trace)
				}
			})
		}
	}
}
