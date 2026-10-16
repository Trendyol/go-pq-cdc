package online

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5"
)

// Store owns a non-reconnecting, session-locked metadata connection. Observe
// Done/Err and cancel the coordinator on loss. Chunk/marker SQL uses a distinct
// connection; no business query or payload belongs on this connection.
type Store struct {
	mu       sync.Mutex
	conn     *pgx.Conn
	slot     string
	terminal error
	done     chan struct{}
	stopped  chan struct{}
	cancel   context.CancelFunc
	pid      uint32
}

func OpenStore(ctx context.Context, dsn, slot string) (*Store, error) {
	return OpenStoreWithStartupTimeout(ctx, dsn, slot, 5*time.Second)
}

// OpenStoreWithStartupTimeout bounds acquisition without binding the watchdog
// to a short-lived query context. ctx owns the entire non-reconnecting lifetime.
func OpenStoreWithStartupTimeout(ctx context.Context, dsn, slot string, timeout time.Duration) (*Store, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("online owner startup timeout must be positive")
	}
	startup, cancelStartup := context.WithTimeout(ctx, timeout)
	defer cancelStartup()
	if slot == "" || strings.ContainsRune(slot, 0) {
		return nil, ErrBinding
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if err = pq.LimitOnlineFrontend(&parsed.Config, 64<<10); err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(startup, parsed)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Store, error) { _ = conn.Close(context.Background()); return nil, err }
	var locked bool
	err = conn.QueryRow(startup, `SELECT pg_try_advisory_lock(hashtextextended(current_database()||':cdc_online:'||$1,0))`, slot).Scan(&locked)
	if err != nil {
		return fail(err)
	}
	if !locked {
		return fail(ErrOwned)
	}
	if err = ValidateProvisioned(startup, conn); err != nil {
		return fail(err)
	}
	run, cancel := context.WithCancel(ctx)
	store := &Store{conn: conn, slot: slot, done: make(chan struct{}), stopped: make(chan struct{}), cancel: cancel, pid: conn.PgConn().PID()}
	go store.watch(run)
	return store, nil
}
func (s *Store) Done() <-chan struct{} { return s.done }
func (s *Store) BackendPID() uint32    { return s.pid }
func (s *Store) Err() error            { s.mu.Lock(); defer s.mu.Unlock(); return s.terminal }
func (s *Store) failLocked(err error) {
	if s.terminal == nil {
		s.terminal = err
		close(s.done)
		s.cancel()
	}
}
func (s *Store) watch(ctx context.Context) {
	defer close(s.stopped)
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			if s.terminal == nil {
				s.failLocked(ctx.Err())
			}
			_ = s.conn.Close(context.Background())
			s.mu.Unlock()
			return
		case <-timer.C:
			s.mu.Lock()
			if s.terminal == nil {
				probe, cancel := context.WithTimeout(ctx, time.Second)
				err := s.conn.Ping(probe)
				cancel()
				if err != nil {
					s.failLocked(errors.Join(ErrOwnerLost, err))
				}
			}
			s.mu.Unlock()
		}
	}
}
func (s *Store) Close(ctx context.Context) error {
	s.mu.Lock()
	s.failLocked(context.Canceled)
	err := s.conn.Close(ctx)
	s.mu.Unlock()
	select {
	case <-s.stopped:
		return err
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}
func (s *Store) transaction(ctx context.Context, external *pgx.Conn, fn func(pgx.Tx) error) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal != nil {
		return s.terminal
	}
	conn := s.conn
	if external != nil {
		if external == s.conn {
			return fmt.Errorf("online marker SQL requires a separate connection")
		}
		if err := s.conn.Ping(ctx); err != nil {
			s.failLocked(errors.Join(ErrOwnerLost, err))
			return s.terminal
		}
		conn = external
	}
	tx, err := conn.Begin(ctx)
	if err == nil {
		defer func() {
			if cleanup := RollbackBounded(tx); cleanup != nil {
				err = errors.Join(err, cleanup)
				if conn == s.conn {
					s.failLocked(errors.Join(ErrOwnerLost, cleanup))
				}
			}
		}()
		// Checkpoints and separately emitted controls must survive a committed
		// response even if the caller's DSN/session opted out of WAL flushes.
		_, err = tx.Exec(ctx, `SET LOCAL synchronous_commit=on`)
		if err == nil {
			err = fn(tx)
		}
		if err == nil {
			err = tx.Commit(ctx)
		}
	}
	if s.conn.IsClosed() {
		s.failLocked(errors.Join(ErrOwnerLost, err))
		return s.terminal
	}
	return err
}

const jobColumns = `job_id::text,connector_id::text,slot_name,slot_binding::text,system_id,database_name,timeline,generation,config_fingerprint,status,source_lsn::text`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var lsn string
	err := row.Scan(&j.ID, &j.ConnectorID, &j.Slot, &j.SlotBinding, &j.Source.SystemID, &j.Source.Database, &j.Source.Timeline, &j.Generation, &j.ConfigFingerprint, &j.Status, &lsn)
	if err != nil {
		return j, err
	}
	j.Checkpoint, err = pq.ParseLSN(lsn)
	return j, err
}
func (s *Store) jobLocked(ctx context.Context, tx pgx.Tx) (Job, error) {
	return scanJob(tx.QueryRow(ctx, "SELECT "+jobColumns+" FROM cdc_online.jobs WHERE slot_name=$1 FOR UPDATE", s.slot))
}
func currentGeneration(j Job, id string, generation int64) error {
	if j.ID != id || j.Generation != generation {
		return ErrConditional
	}
	return nil
}
func (s *Store) LoadJob(ctx context.Context) (j Job, err error) {
	err = s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, err = scanJob(tx.QueryRow(ctx, "SELECT "+jobColumns+" FROM cdc_online.jobs WHERE slot_name=$1", s.slot))
		return err
	})
	return
}

// CreateJob records caller-certified fresh-slot provenance. It never adopts an
// existing slot/job; the slot helper/coordinator must reconcile partial setup.
func (s *Store) CreateJob(ctx context.Context, j Job) error {
	for _, id := range []string{j.ID, j.ConnectorID, j.SlotBinding} {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if j.Slot != s.slot || j.Generation != 1 || j.Checkpoint == 0 || j.Source.SystemID == "" || j.Source.Database == "" || j.Source.Timeline <= 0 || j.ConfigFingerprint == "" {
		return ErrBinding
	}
	if j.Status != "" && j.Status != "active" {
		return ErrBinding
	}
	marker, err := NewID()
	if err != nil {
		return err
	}
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO cdc_online.jobs(job_id,connector_id,slot_name,slot_binding,system_id,database_name,timeline,generation,config_fingerprint,status,source_lsn) VALUES($1::uuid,$2::uuid,$3,$4::uuid,$5,$6,$7,1,$8,'active',$9::pg_lsn)`, j.ID, j.ConnectorID, j.Slot, j.SlotBinding, j.Source.SystemID, j.Source.Database, j.Source.Timeline, j.ConfigFingerprint, j.Checkpoint.String())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO cdc_online.watermarks(connector_id,job_id,generation,attempt_id,phase,marker_id) VALUES($1::uuid,$2::uuid,1,NULL,'HEARTBEAT',$3::uuid)`, j.ConnectorID, j.ID, marker)
		return err
	})
}
func sameBinding(a, b Job) bool {
	return a.ID == b.ID && a.ConnectorID == b.ConnectorID && a.Slot == b.Slot && a.SlotBinding == b.SlotBinding && a.Source == b.Source && a.ConfigFingerprint == b.ConfigFingerprint
}

// ResumeJob fences late completions and abandons all volatile old attempts in
// the same transaction. Load the current binding and validate real slot/source
// facts with ValidateResume before calling it. It never changes the checkpoint.
func (s *Store) ResumeJob(ctx context.Context, expected Job) (j Job, err error) {
	err = s.transaction(ctx, nil, func(tx pgx.Tx) error {
		current, e := s.jobLocked(ctx, tx)
		if e != nil {
			return e
		}
		if !sameBinding(current, expected) {
			return ErrBinding
		}
		if current.Generation != expected.Generation {
			return ErrConditional
		}
		if _, e = tx.Exec(ctx, `UPDATE cdc_online.windows SET status='abandoned' WHERE job_id=$1::uuid AND status IN ('registered','low','high')`, current.ID); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE cdc_online.jobs SET generation=generation+1 WHERE job_id=$1::uuid`, current.ID); e != nil {
			return e
		}
		if e = pruneWindows(ctx, tx, current.ID); e != nil {
			return e
		}
		j = current
		j.Generation++
		return nil
	})
	return
}
func (s *Store) InitTable(ctx context.Context, jobID string, generation int64, p TableProgress) error {
	if p.JobID != jobID || p.Table.Schema == "" || p.Table.Name == "" || p.SchemaFingerprint == "" || p.KeyFingerprint == "" || p.Cursor != nil || p.Complete {
		return ErrConditional
	}
	if p.Table.Schema == "cdc_online" {
		return ErrConditional
	}
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, err := s.jobLocked(ctx, tx)
		if err != nil {
			return err
		}
		if err = currentGeneration(j, jobID, generation); err != nil {
			return err
		}
		if j.Status != "active" {
			return ErrConditional
		}
		_, err = tx.Exec(ctx, `INSERT INTO cdc_online.table_progress(job_id,table_schema,table_name,schema_fingerprint,key_fingerprint,upper_key) VALUES($1::uuid,$2,$3,$4,$5,$6)`, jobID, p.Table.Schema, p.Table.Name, p.SchemaFingerprint, p.KeyFingerprint, p.UpperKey)
		return err
	})
}

const tableColumns = `job_id::text,table_schema,table_name,schema_fingerprint,key_fingerprint,upper_key,cursor_key,complete`

func scanTable(row pgx.Row) (TableProgress, error) {
	var p TableProgress
	err := row.Scan(&p.JobID, &p.Table.Schema, &p.Table.Name, &p.SchemaFingerprint, &p.KeyFingerprint, &p.UpperKey, &p.Cursor, &p.Complete)
	return p, err
}
func getTable(ctx context.Context, tx pgx.Tx, jobID string, table Table) (TableProgress, error) {
	return scanTable(tx.QueryRow(ctx, "SELECT "+tableColumns+" FROM cdc_online.table_progress WHERE job_id=$1::uuid AND table_schema=$2 AND table_name=$3 FOR UPDATE", jobID, table.Schema, table.Name))
}
func (s *Store) GetTable(ctx context.Context, jobID string, table Table) (p TableProgress, err error) {
	err = s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, e := s.jobLocked(ctx, tx)
		if e != nil {
			return e
		}
		if j.ID != jobID {
			return ErrBinding
		}
		p, e = getTable(ctx, tx, jobID, table)
		return e
	})
	return
}
func (s *Store) Tables(ctx context.Context, jobID string) (tables []TableProgress, err error) {
	err = s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, e := s.jobLocked(ctx, tx)
		if e != nil {
			return e
		}
		if j.ID != jobID {
			return ErrBinding
		}
		rows, e := tx.Query(ctx, "SELECT "+tableColumns+" FROM cdc_online.table_progress WHERE job_id=$1::uuid ORDER BY table_schema,table_name", jobID)
		if e != nil {
			return e
		}
		defer rows.Close()
		for rows.Next() {
			p, e := scanTable(rows)
			if e != nil {
				return e
			}
			tables = append(tables, p)
		}
		return rows.Err()
	})
	return
}
func sameKey(a, b *int64) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

// MaxDiagnosticWindows bounds completed/abandoned history. An identity older
// than this history is unknown and fails closed; it must never be retried as a
// new completion. The single active attempt is retained independently.
const MaxDiagnosticWindows = 16

func pruneWindows(ctx context.Context, tx pgx.Tx, jobID string) error {
	_, err := tx.Exec(ctx, `DELETE FROM cdc_online.windows WHERE job_id=$1::uuid AND status IN ('completed','abandoned') AND attempt_id NOT IN (SELECT attempt_id FROM cdc_online.windows WHERE job_id=$1::uuid AND status IN ('completed','abandoned') ORDER BY created_at DESC,attempt_id DESC LIMIT $2)`, jobID, MaxDiagnosticWindows)
	return err
}
func (s *Store) RegisterAttempt(ctx context.Context, w Window) error {
	for _, id := range []string{w.ID, w.JobID, w.LowID, w.HighID} {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if w.LowID == w.HighID || w.LastKey != nil || w.LowEnd != 0 || w.HighEnd != 0 || w.LowEmitted || w.HighEmitted || (w.Status != "" && w.Status != "registered") {
		return ErrConditional
	}
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, err := s.jobLocked(ctx, tx)
		if err != nil {
			return err
		}
		if err = currentGeneration(j, w.JobID, w.Generation); err != nil {
			return err
		}
		if j.Status != "active" {
			return ErrConditional
		}
		p, err := getTable(ctx, tx, w.JobID, w.Table)
		if err != nil {
			return err
		}
		if p.Complete || !sameKey(p.Cursor, w.PreviousCursor) {
			return ErrConditional
		}
		_, err = tx.Exec(ctx, `INSERT INTO cdc_online.windows(attempt_id,job_id,generation,table_schema,table_name,previous_cursor,low_id,high_id,status) VALUES($1::uuid,$2::uuid,$3,$4,$5,$6,$7::uuid,$8::uuid,'registered')`, w.ID, w.JobID, w.Generation, w.Table.Schema, w.Table.Name, w.PreviousCursor, w.LowID, w.HighID)
		if err != nil {
			return err
		}
		return pruneWindows(ctx, tx, w.JobID)
	})
}

const windowColumns = `attempt_id::text,job_id::text,generation,table_schema,table_name,previous_cursor,last_key,low_id::text,high_id::text,COALESCE(low_end,'0/0'::pg_lsn)::text,COALESCE(high_end,'0/0'::pg_lsn)::text,low_emitted,high_emitted,status`

func scanWindow(row pgx.Row) (Window, error) {
	var w Window
	var low, high string
	err := row.Scan(&w.ID, &w.JobID, &w.Generation, &w.Table.Schema, &w.Table.Name, &w.PreviousCursor, &w.LastKey, &w.LowID, &w.HighID, &low, &high, &w.LowEmitted, &w.HighEmitted, &w.Status)
	if err != nil {
		return w, err
	}
	w.LowEnd, err = pq.ParseLSN(low)
	if err != nil {
		return w, err
	}
	w.HighEnd, err = pq.ParseLSN(high)
	return w, err
}
func getWindow(ctx context.Context, tx pgx.Tx, id, jobID string) (Window, error) {
	return scanWindow(tx.QueryRow(ctx, "SELECT "+windowColumns+" FROM cdc_online.windows WHERE attempt_id=$1::uuid AND job_id=$2::uuid FOR UPDATE", id, jobID))
}

// GetAttempt resolves an unknown commit response by original identity. It also
// exposes abandoned old generations; it does not silently retry a transition.
func (s *Store) GetAttempt(ctx context.Context, id string) (w Window, err error) {
	err = s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, e := s.jobLocked(ctx, tx)
		if e != nil {
			return e
		}
		w, e = getWindow(ctx, tx, id, j.ID)
		return e
	})
	return
}
func (s *Store) activeWindow(ctx context.Context, tx pgx.Tx, id string, generation int64) (Job, Window, error) {
	j, err := s.jobLocked(ctx, tx)
	if err != nil {
		return j, Window{}, err
	}
	w, err := getWindow(ctx, tx, id, j.ID)
	if err != nil {
		return j, w, err
	}
	if j.Generation != generation || w.Generation != generation || w.JobID != j.ID {
		return j, w, ErrConditional
	}
	return j, w, nil
}
func (s *Store) RecordLOW(ctx context.Context, id string, generation int64, markerID string, end pq.LSN) error {
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, w, err := s.activeWindow(ctx, tx, id, generation)
		if err != nil {
			return err
		}
		if w.Status != "registered" || !w.LowEmitted || markerID != w.LowID || end == 0 || end <= j.Checkpoint {
			return ErrConditional
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.windows SET status='low',low_end=$2::pg_lsn WHERE attempt_id=$1::uuid AND status='registered'`, id, end.String())
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		result, err = tx.Exec(ctx, `UPDATE cdc_online.jobs SET source_lsn=$2::pg_lsn WHERE job_id=$1::uuid AND generation=$3 AND source_lsn<=$2::pg_lsn`, j.ID, end.String(), generation)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return nil
	})
}
func (s *Store) RecordHIGH(ctx context.Context, id string, generation int64, markerID string, end pq.LSN, last *int64) error {
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		_, w, err := s.activeWindow(ctx, tx, id, generation)
		if err != nil {
			return err
		}
		if w.Status != "low" || !w.HighEmitted || markerID != w.HighID || end <= w.LowEnd {
			return ErrConditional
		}
		p, err := getTable(ctx, tx, w.JobID, w.Table)
		if err != nil {
			return err
		}
		if !sameKey(p.Cursor, w.PreviousCursor) || p.Complete || (last != nil && (p.UpperKey == nil || *last > *p.UpperKey || (p.Cursor != nil && *last <= *p.Cursor))) {
			return ErrConditional
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.windows SET status='high',high_end=$2::pg_lsn,last_key=$3 WHERE attempt_id=$1::uuid AND status='low'`, id, end.String(), last)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return nil
	})
}

// CompleteAttempt must be called only after every delivery obligation through
// HIGH is durable. Storage cannot certify an arbitrary handler's durability.
func (s *Store) CompleteAttempt(ctx context.Context, id string, generation int64, tableComplete bool) error {
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, w, err := s.activeWindow(ctx, tx, id, generation)
		if err != nil {
			return err
		}
		if w.Status != "high" || w.HighEnd == 0 || w.HighEnd < j.Checkpoint || (w.LastKey == nil && !tableComplete) {
			return ErrConditional
		}
		p, err := getTable(ctx, tx, w.JobID, w.Table)
		if err != nil {
			return err
		}
		if p.Complete || !sameKey(p.Cursor, w.PreviousCursor) {
			return ErrConditional
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.windows SET status='completed' WHERE attempt_id=$1::uuid AND status='high'`, id)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		result, err = tx.Exec(ctx, `UPDATE cdc_online.table_progress SET cursor_key=COALESCE($4,cursor_key),complete=$5 WHERE job_id=$1::uuid AND table_schema=$2 AND table_name=$3 AND NOT complete AND cursor_key IS NOT DISTINCT FROM $6`, w.JobID, w.Table.Schema, w.Table.Name, w.LastKey, tableComplete, w.PreviousCursor)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		result, err = tx.Exec(ctx, `UPDATE cdc_online.jobs SET source_lsn=$2::pg_lsn WHERE job_id=$1::uuid AND generation=$3 AND source_lsn<=$2::pg_lsn`, j.ID, w.HighEnd.String(), generation)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return pruneWindows(ctx, tx, j.ID)
	})
}
func (s *Store) AbandonAttempt(ctx context.Context, id string, generation int64) error {
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, w, err := s.activeWindow(ctx, tx, id, generation)
		if err != nil {
			return err
		}
		if w.Status != "registered" && w.Status != "low" && w.Status != "high" {
			return ErrConditional
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.windows SET status='abandoned' WHERE attempt_id=$1::uuid AND status IN ('registered','low','high')`, id)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return pruneWindows(ctx, tx, j.ID)
	})
}

// Checkpoint deliberately forbids all active windows. LOW and HIGH checkpoints
// go through their atomic attempt APIs; heartbeat/empty transactions cannot
// bypass the unresolved LOW ceiling.
func (s *Store) Checkpoint(ctx context.Context, jobID string, generation int64, end pq.LSN) error {
	if end == 0 {
		return ErrConditional
	}
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, err := s.jobLocked(ctx, tx)
		if err != nil {
			return err
		}
		if err = currentGeneration(j, jobID, generation); err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.jobs SET source_lsn=$2::pg_lsn WHERE job_id=$1::uuid AND generation=$3 AND source_lsn<=$2::pg_lsn AND NOT EXISTS(SELECT 1 FROM cdc_online.windows WHERE job_id=$1::uuid AND status IN ('registered','low','high'))`, jobID, end.String(), generation)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return nil
	})
}
func (s *Store) MarkSnapshotComplete(ctx context.Context, jobID string, generation int64) error {
	return s.transaction(ctx, nil, func(tx pgx.Tx) error {
		j, err := s.jobLocked(ctx, tx)
		if err != nil {
			return err
		}
		if err = currentGeneration(j, jobID, generation); err != nil {
			return err
		}
		result, err := tx.Exec(ctx, `UPDATE cdc_online.jobs SET status='snapshot_complete' WHERE job_id=$1::uuid AND status='active' AND NOT EXISTS(SELECT 1 FROM cdc_online.table_progress WHERE job_id=$1::uuid AND NOT complete) AND NOT EXISTS(SELECT 1 FROM cdc_online.windows WHERE job_id=$1::uuid AND status IN ('registered','low','high'))`, jobID)
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		return nil
	})
}

// EmitLOW and the other Emit methods commit their own transaction on the chunk/marker
// connection. No HIGH is emitted until a separately committed LOW was observed.
// Emission flags and the watermark UPDATE commit together, but only RecordLOW /
// RecordHIGH persist the visibility-gated source transaction-end boundary.
func (s *Store) EmitLOW(ctx context.Context, sql *pgx.Conn, id string, generation int64) (Marker, error) {
	return s.emitWindow(ctx, sql, id, generation, Low)
}
func (s *Store) EmitHIGH(ctx context.Context, sql *pgx.Conn, id string, generation int64) (Marker, error) {
	return s.emitWindow(ctx, sql, id, generation, High)
}
func (s *Store) emitWindow(ctx context.Context, sql *pgx.Conn, id string, generation int64, phase MarkerPhase) (marker Marker, err error) {
	if sql == nil {
		return marker, fmt.Errorf("online marker SQL connection is required")
	}
	err = s.transaction(ctx, sql, func(tx pgx.Tx) error {
		j, w, e := s.activeWindow(ctx, tx, id, generation)
		if e != nil {
			return e
		}
		var statement, markerID string
		if phase == Low {
			if w.Status != "registered" || w.LowEmitted {
				return ErrConditional
			}
			statement = `UPDATE cdc_online.windows SET low_emitted=true WHERE attempt_id=$1::uuid AND status='registered' AND NOT low_emitted`
			markerID = w.LowID
		} else {
			if w.Status != "low" || w.HighEmitted {
				return ErrConditional
			}
			statement = `UPDATE cdc_online.windows SET high_emitted=true WHERE attempt_id=$1::uuid AND status='low' AND NOT high_emitted`
			markerID = w.HighID
		}
		result, e := tx.Exec(ctx, statement, id)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		result, e = tx.Exec(ctx, `UPDATE cdc_online.watermarks SET job_id=$2::uuid,generation=$3,attempt_id=$4::uuid,phase=$5,marker_id=$6::uuid WHERE connector_id=$1::uuid AND job_id=$2::uuid`, j.ConnectorID, j.ID, generation, id, string(phase), markerID)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		marker = Marker{ConnectorID: j.ConnectorID, JobID: j.ID, Generation: generation, AttemptID: id, Phase: phase, ID: markerID}
		return nil
	})
	return marker, err
}
func (s *Store) EmitHeartbeat(ctx context.Context, sql *pgx.Conn, jobID string, generation int64) (marker Marker, err error) {
	if sql == nil {
		return marker, fmt.Errorf("online marker SQL connection is required")
	}
	id, err := NewID()
	if err != nil {
		return marker, err
	}
	err = s.transaction(ctx, sql, func(tx pgx.Tx) error {
		j, e := s.jobLocked(ctx, tx)
		if e != nil {
			return e
		}
		if e = currentGeneration(j, jobID, generation); e != nil {
			return e
		}
		result, e := tx.Exec(ctx, `UPDATE cdc_online.watermarks SET generation=$3,attempt_id=NULL,phase='HEARTBEAT',marker_id=$4::uuid WHERE connector_id=$1::uuid AND job_id=$2::uuid AND NOT EXISTS(SELECT 1 FROM cdc_online.windows WHERE job_id=$2::uuid AND status IN ('registered','low','high'))`, j.ConnectorID, j.ID, generation, id)
		if e != nil {
			return e
		}
		if result.RowsAffected() != 1 {
			return ErrConditional
		}
		marker = Marker{ConnectorID: j.ConnectorID, JobID: j.ID, Generation: generation, Phase: Heartbeat, ID: id}
		return nil
	})
	return
}
