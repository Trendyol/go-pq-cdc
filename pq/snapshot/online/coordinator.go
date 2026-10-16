package online

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/jackc/pgx/v5"
)

type CoordinatorOptions struct {
	Publication                                                       string
	Selected                                                          []Table
	ChunkRows                                                         int
	ChunkBytes                                                        int64
	QueryTimeout, DeliveryTimeout, CheckpointInterval, MarkerInterval time.Duration
	SnapshotComplete                                                  func()
	Observer                                                          Observer
	Counters                                                          *Counters
}

// Coordinator is the sole SQL/merge/handler owner. Reader I/O and its visibility
// gate remain independently managed. One handoff envelope plus one chunk are
// retained here; there is no row-payload outbox or secondary unbounded queue.
type Coordinator struct {
	Store    *Store
	SQL      *pgx.Conn
	Reader   *replication.OnlineReader
	Job      Job
	Schemas  []TableSchema
	Handler  Handler
	Options  CoordinatorOptions
	incoming chan *replication.OnlineCommittedEnvelope
	request  chan struct{}
}

func (c *Coordinator) observe(ctx context.Context, phase ObservationPhase, attempt Window, end pq.LSN, selected int, last *int64) error {
	if c.Options.Observer == nil {
		return ctx.Err()
	}
	observation := Observation{Phase: phase, JobID: c.Job.ID, AttemptID: attempt.ID, Generation: c.Job.Generation, Table: attempt.Table, EndLSN: end, SelectedRows: selected}
	if last != nil {
		observation.HasLastKey = true
		observation.LastKey = *last
	}
	call, cancel := context.WithTimeout(ctx, c.Options.DeliveryTimeout)
	defer cancel()
	if err := c.Options.Observer(call, observation); err != nil {
		return err
	}
	return call.Err()
}
func (c *Coordinator) bounded(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, c.Options.QueryTimeout)
	defer cancel()
	return fn(ctx)
}
func (c *Coordinator) checkSchema(ctx context.Context) error {
	return c.bounded(ctx, func(ctx context.Context) error {
		actual, err := ValidatePublication(ctx, c.SQL, c.Options.Publication)
		if err != nil {
			return err
		}
		return ValidateSchemas(c.Schemas, actual)
	})
}
func (c *Coordinator) apply(ctx context.Context, e Event) error {
	select {
	case <-c.Store.Done():
		return c.Store.Err()
	case <-c.Reader.Done():
		return c.Reader.Err()
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.Validate(); err != nil {
		return err
	}
	delivery, cancel := context.WithTimeout(ctx, c.Options.DeliveryTimeout)
	defer cancel()
	if err := c.Handler(delivery, e); err != nil {
		return fmt.Errorf("online durable handler: %w", err)
	}
	if err := delivery.Err(); err != nil {
		return fmt.Errorf("online durable handler deadline: %w", err)
	}
	return ctx.Err()
}
func (c *Coordinator) requestNext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.Store.Done():
		return c.Store.Err()
	case <-c.Reader.Done():
		return c.Reader.Err()
	case c.request <- struct{}{}:
		return nil
	}
}
func (c *Coordinator) next(ctx context.Context) (*replication.OnlineCommittedEnvelope, error) {
	if err := c.requestNext(ctx); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.Store.Done():
		return nil, c.Store.Err()
	case <-c.Reader.Done():
		return nil, c.Reader.Err()
	case env := <-c.incoming:
		if env == nil {
			return nil, errors.New("online envelope handoff closed")
		}
		return env, nil
	}
}
func (c *Coordinator) ack(end pq.LSN) error {
	if err := c.Reader.AdvanceSafeACK(end); err != nil {
		return err
	}
	c.Job.Checkpoint = end
	return nil
}
func (c *Coordinator) process(ctx context.Context, env *replication.OnlineCommittedEnvelope, w *ReadWindow, expected *Marker) (bool, error) {
	if err := c.checkSchema(ctx); err != nil {
		return false, err
	}
	// Validate the entire envelope BEFORE releasing any event. An unsupported
	// key/image/control in a later ordinal must not escape as partial delivery.
	events := make([]Event, 0, len(env.Changes))
	matched := false
	for _, change := range env.Changes {
		marker, control, err := DecodeControl(change)
		if err != nil {
			return false, err
		}
		if control {
			found, err := matchMarker(c.Job, marker, expected, len(env.Changes))
			if err != nil {
				return false, err
			}
			if found && matched {
				return false, ErrConditional
			}
			matched = matched || found
			continue
		}
		event, err := BusinessEvent(change, env, c.Schemas, c.Job.SlotBinding)
		if err != nil {
			return false, err
		}
		events = append(events, event)
	}
	for _, event := range events {
		if w != nil {
			w.Conflict(event.Table, event.Key)
		}
		if err := c.apply(ctx, event); err != nil {
			return false, err
		}
	}
	return matched, nil
}
func (c *Coordinator) through(ctx context.Context, marker Marker, w *ReadWindow) (pq.LSN, error) {
	for {
		env, err := c.next(ctx)
		if err != nil {
			return 0, err
		}
		found, err := c.process(ctx, env, w, &marker)
		if err != nil {
			return 0, err
		}
		if found {
			return env.EndLSN, nil
		}
	}
}
func (c *Coordinator) tableSchema(table Table) (TableSchema, error) {
	for _, s := range c.Schemas {
		if s.Table == table {
			return s, nil
		}
	}
	return TableSchema{}, ErrSchema
}
func (c *Coordinator) window(ctx context.Context, p TableProgress) error {
	s, err := c.tableSchema(p.Table)
	if err != nil {
		return err
	}
	if s.Fingerprint != p.SchemaFingerprint || s.KeyFingerprint != p.KeyFingerprint {
		return ErrSchema
	}
	id, err := NewID()
	if err != nil {
		return err
	}
	lowID, err := NewID()
	if err != nil {
		return err
	}
	highID, err := NewID()
	if err != nil {
		return err
	}
	attempt := Window{ID: id, JobID: c.Job.ID, Generation: c.Job.Generation, Table: p.Table, PreviousCursor: p.Cursor, LowID: lowID, HighID: highID}
	if err = c.bounded(ctx, func(ctx context.Context) error { return c.Store.RegisterAttempt(ctx, attempt) }); err != nil {
		return err
	}
	var marker Marker
	if err = c.bounded(ctx, func(ctx context.Context) error {
		marker, err = c.Store.EmitLOW(ctx, c.SQL, id, c.Job.Generation)
		return err
	}); err != nil {
		return err
	}
	low, err := c.through(ctx, marker, nil)
	if err != nil {
		return err
	}
	if err = c.bounded(ctx, func(ctx context.Context) error { return c.Store.RecordLOW(ctx, id, c.Job.Generation, lowID, low) }); err != nil {
		return err
	}
	if err = c.observe(ctx, LOWRecorded, attempt, low, 0, nil); err != nil {
		return err
	}
	if err = c.ack(low); err != nil {
		return err
	}
	// Dispatch is frozen from observed/visible LOW until SELECT and separately
	// committed HIGH. The source reader still receives and sends safe feedback.
	if err = c.checkSchema(ctx); err != nil {
		return err
	}
	if err = c.observe(ctx, ReadStarted, attempt, low, 0, nil); err != nil { //nolint:gofmt
		return err
	}
	rows, err := ReadPage(ctx, c.SQL, s, p.Cursor, p.UpperKey, c.Options.ChunkRows, c.Options.ChunkBytes, c.Options.QueryTimeout, c.Options.Counters)
	if err != nil {
		abandonErr := c.bounded(ctx, func(ctx context.Context) error { return c.Store.AbandonAttempt(ctx, id, c.Job.Generation) })
		return errors.Join(err, abandonErr)
	}
	w := NewReadWindow(p.Table, rows)
	if err = c.observe(ctx, ReadResult, attempt, low, len(rows), w.LastKey()); err != nil {
		return err
	}
	if err = c.bounded(ctx, func(ctx context.Context) error {
		marker, err = c.Store.EmitHIGH(ctx, c.SQL, id, c.Job.Generation)
		return err
	}); err != nil {
		return err
	}
	high, err := c.through(ctx, marker, w)
	if err != nil {
		return err
	}
	if err = c.bounded(ctx, func(ctx context.Context) error {
		return c.Store.RecordHIGH(ctx, id, c.Job.Generation, highID, high, w.LastKey())
	}); err != nil {
		return err
	}
	if err = c.checkSchema(ctx); err != nil {
		return err
	}
	if err = c.observe(ctx, HIGHRecorded, attempt, high, len(rows), w.LastKey()); err != nil {
		return err
	}
	for _, event := range w.Reads(c.Job.ID, id, high) {
		if err = c.apply(ctx, event); err != nil {
			return err
		}
	}
	if err = c.observe(ctx, ReadsDelivered, attempt, high, len(rows), w.LastKey()); err != nil {
		return err
	}
	complete := len(rows) < c.Options.ChunkRows || (w.LastKey() != nil && p.UpperKey != nil && *w.LastKey() == *p.UpperKey)
	if err = c.bounded(ctx, func(ctx context.Context) error { return c.Store.CompleteAttempt(ctx, id, c.Job.Generation, complete) }); err != nil {
		// A lost metadata reply is resolved by original identity, never by replaying
		// an unconditional transition. A broken non-reconnecting store fails closed.
		var persisted Window
		lookup := c.bounded(ctx, func(ctx context.Context) error { var e error; persisted, e = c.Store.GetAttempt(ctx, id); return e })
		if lookup != nil || persisted.Status != "completed" || persisted.Generation != c.Job.Generation || persisted.HighEnd != high {
			return errors.Join(err, lookup)
		}
	}
	if err = c.observe(ctx, CompletionCommitted, attempt, high, len(rows), w.LastKey()); err != nil {
		return err
	}
	if err = c.ack(high); err != nil {
		return err
	}
	return c.observe(ctx, ACKAdvanced, attempt, high, len(rows), w.LastKey())
}
func (c *Coordinator) Run(ctx context.Context) error {
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	c.incoming = make(chan *replication.OnlineCommittedEnvelope)
	c.request = make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-c.request:
			case <-run.Done():
				return
			}
			env, err := c.Reader.Next(run)
			if err != nil {
				return
			}
			select {
			case c.incoming <- env:
			case <-run.Done():
				return
			}
		}
	}()
	defer func() { cancel(); <-stopped }()
	// Capture has started before upper bounds. Init is idempotent only by explicit
	// durable table existence; resumed jobs must retain the same fingerprints.
	for _, table := range c.Options.Selected {
		s, err := c.tableSchema(table)
		if err != nil {
			return err
		}
		var p TableProgress
		err = c.bounded(run, func(ctx context.Context) error { var e error; p, e = c.Store.GetTable(ctx, c.Job.ID, table); return e })
		if errors.Is(err, pgx.ErrNoRows) {
			var upper *int64
			if err = c.bounded(run, func(ctx context.Context) error { var e error; upper, e = CaptureUpper(ctx, c.SQL, s); return e }); err != nil {
				return err
			}
			p = TableProgress{JobID: c.Job.ID, Table: table, SchemaFingerprint: s.Fingerprint, KeyFingerprint: s.KeyFingerprint, UpperKey: upper}
			if err = c.bounded(run, func(ctx context.Context) error { return c.Store.InitTable(ctx, c.Job.ID, c.Job.Generation, p) }); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if p.SchemaFingerprint != s.Fingerprint || p.KeyFingerprint != s.KeyFingerprint {
			return ErrSchema
		}
		for !p.Complete {
			if err = c.window(run, p); err != nil {
				return err
			}
			if err = c.bounded(run, func(ctx context.Context) error { var e error; p, e = c.Store.GetTable(ctx, c.Job.ID, table); return e }); err != nil {
				return err
			}
		}
	}
	if c.Job.Status != "snapshot_complete" {
		if err := c.bounded(run, func(ctx context.Context) error { return c.Store.MarkSnapshotComplete(ctx, c.Job.ID, c.Job.Generation) }); err != nil {
			return err
		}
	}
	if c.Options.SnapshotComplete != nil {
		c.Options.SnapshotComplete()
	}
	checkpoint := time.NewTicker(c.Options.CheckpointInterval)
	defer checkpoint.Stop()
	heartbeat := time.NewTicker(c.Options.MarkerInterval)
	defer heartbeat.Stop()
	frontier := c.Job.Checkpoint
	if err := c.requestNext(run); err != nil {
		return err
	}
	for {
		select {
		case <-run.Done():
			return run.Err()
		case <-c.Store.Done():
			return c.Store.Err()
		case <-c.Reader.Done():
			return c.Reader.Err()
		case env := <-c.incoming:
			if _, err := c.process(run, env, nil, nil); err != nil {
				return err
			}
			frontier = env.EndLSN
			if err := c.requestNext(run); err != nil {
				return err
			}
		case <-heartbeat.C:
			if err := c.bounded(run, func(ctx context.Context) error {
				_, e := c.Store.EmitHeartbeat(ctx, c.SQL, c.Job.ID, c.Job.Generation)
				return e
			}); err != nil {
				return err
			}
		case <-checkpoint.C:
			if frontier > c.Job.Checkpoint {
				if err := c.bounded(run, func(ctx context.Context) error { return c.Store.Checkpoint(ctx, c.Job.ID, c.Job.Generation, frontier) }); err != nil {
					return err
				}
				if err := c.ack(frontier); err != nil {
					return err
				}
			}
		}
	}
}
