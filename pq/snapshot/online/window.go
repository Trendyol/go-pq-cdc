package online

import "github.com/Trendyol/go-pq-cdc/pq"

type SelectedRow struct {
	Key   int64
	Image map[string]any
}

// ReadWindow retains only one bounded SELECT. Conflict and Reads are called by
// the single coordinator, never concurrently with appending a selected row.
type ReadWindow struct {
	table     Table
	rows      []SelectedRow
	remaining map[int64]bool
}

func NewReadWindow(table Table, rows []SelectedRow) *ReadWindow {
	w := &ReadWindow{table: table, rows: rows, remaining: make(map[int64]bool, len(rows))}
	for _, row := range rows {
		w.remaining[row.Key] = true
	}
	return w
}
func (w *ReadWindow) Conflict(table Table, key int64) {
	if table == w.table {
		delete(w.remaining, key)
	}
}
func (w *ReadWindow) LastKey() *int64 {
	if len(w.rows) == 0 {
		return nil
	}
	k := w.rows[len(w.rows)-1].Key
	return &k
}
func (w *ReadWindow) Reads(job, attempt string, high pq.LSN) []Event {
	events := make([]Event, 0, len(w.remaining))
	for ordinal, row := range w.rows {
		if !w.remaining[row.Key] {
			continue
		}
		events = append(events, Event{ID: READEventID(job, attempt, w.table, row.Key), Table: w.table, Key: row.Key, Operation: Read, Image: row.Image, Order: OrderToken{EndLSN: high, Phase: 1, Ordinal: uint32(ordinal)}})
	}
	return events
}
