package cdc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
)

type OnlineObservation = online.Observation
type OnlineStats = online.Stats
type onlineConnectorOptions struct{ observer online.Observer }
type OnlineConnectorOption func(*onlineConnectorOptions)

// WithOnlineObserver observes immutable metadata synchronously outside locks.
// It must honor ctx; errors stop Run without rolling back an already committed
// checkpoint/ACK. Default nil has no observer behavior.
func WithOnlineObserver(observer online.Observer) OnlineConnectorOption {
	return func(options *onlineConnectorOptions) { options.observer = observer }
}

type OnlineEvent = online.Event
type OnlineHandler = online.Handler
type OnlineOrderToken = online.OrderToken
type OnlineTable = online.Table
type OnlineOperation = online.Operation

const (
	OnlineInsert = online.Insert
	OnlineUpdate = online.Update
	OnlineDelete = online.Delete
	OnlineRead   = online.Read
)

// OnlineConnector is opt-in, primary-only PostgreSQL, protocol 1. Run invokes
// one handler at a time. Nil MUST mean durable completion, not enqueue success.
// Handlers must honor cancellation; Go cannot terminate an arbitrary callback.
// Readiness means capture started / snapshot delivered, not production safety.
type OnlineConnector struct {
	cfg                     config.Config
	handler                 OnlineHandler
	mu                      sync.Mutex
	started                 bool
	terminal                error
	observer                online.Observer
	counters                online.Counters
	reader                  *replication.OnlineReader
	capture, snapshot, done chan struct{}
}

func NewOnlineConnector(ctx context.Context, cfg config.Config, handler OnlineHandler, opts ...OnlineConnectorOption) (*OnlineConnector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg.Publication.Tables = append(publication.Tables(nil), cfg.Publication.Tables...)
	cfg.Snapshot.Tables = append(publication.Tables(nil), cfg.Snapshot.Tables...)
	cfg.SetDefault()
	if cfg.Snapshot.Strategy != config.SnapshotStrategyOnline {
		return nil, errors.New("NewOnlineConnector requires snapshot.strategy online")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("online configuration: %w", err)
	}
	if cfg.Snapshot.Online.TransactionBytes < 47 {
		return nil, errors.New("online transaction budget must admit BEGIN/COMMIT (47 bytes)")
	}
	for _, budget := range []int64{cfg.Snapshot.Online.ChunkBytes, cfg.Snapshot.Online.QueueBytes} {
		if _, err := pq.OnlineBodyLimit(budget); err != nil {
			return nil, err
		}
	}
	if handler == nil {
		return nil, errors.New("online requires a synchronous durable handler")
	}
	// Defaults only mutate owned copies. Credentials are never fingerprinted.
	options := onlineConnectorOptions{}
	for _, option := range opts {
		if option != nil {
			option(&options)
		}
	}
	return &OnlineConnector{cfg: cfg, handler: handler, observer: options.observer, capture: make(chan struct{}), snapshot: make(chan struct{}), done: make(chan struct{})}, nil
}

// Stats returns a thread-safe bounded snapshot. Bytes are encoded/raw retained
// work, not total heap/RSS; process metrics must be measured by the caller.
func (c *OnlineConnector) Stats() OnlineStats {
	s := c.counters.Snapshot()
	c.mu.Lock()
	reader := c.reader
	c.mu.Unlock()
	if reader != nil {
		r := reader.Stats()
		s.PeakTransactionEncodedBytes = r.PeakTransactionEncodedBytes
		s.PeakQueueAndAssemblyEncodedBytes = r.PeakQueueAndAssemblyEncodedBytes
		s.PeakRelationEncodedBytes = r.PeakRelationEncodedBytes
		s.BufferedEncodedBytes = r.BufferedEncodedBytes
		s.SafeACK = r.SafeACK
		s.FeedbackWrites = r.FeedbackWrites
		s.LastFeedback = r.LastFeedback
	}
	return s
}
func (c *OnlineConnector) Done() <-chan struct{} { return c.done }
func (c *OnlineConnector) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.terminal }
func (c *OnlineConnector) wait(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.Err()
	case <-ch:
		if err := c.Err(); err != nil {
			return err
		}
		return nil
	}
}
func (c *OnlineConnector) WaitUntilCaptureReady(ctx context.Context) error {
	return c.wait(ctx, c.capture)
}
func (c *OnlineConnector) WaitUntilSnapshotComplete(ctx context.Context) error {
	return c.wait(ctx, c.snapshot)
}
func (c *OnlineConnector) query(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Snapshot.Online.QueryTimeout)
	defer cancel()
	return fn(ctx)
}
func (c *OnlineConnector) fingerprint(schemas []online.TableSchema) string {
	data, _ := json.Marshal(struct {
		Publication string
		Tables      publication.Tables
		Snapshot    config.SnapshotConfig
		Protocol    int
		Schemas     []online.TableSchema
	}{c.cfg.Publication.Name, c.cfg.Publication.Tables, c.cfg.Snapshot, c.cfg.Slot.ProtoVersion, schemas})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
func (c *OnlineConnector) setupPublication(ctx context.Context, conn *pgx.Conn) error {
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", c.cfg.Publication.Name).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if !c.cfg.Publication.CreateIfNotExists {
			return errors.New("online publication missing; pre-provision it or enable publication.createIfNotExists")
		}
		if len(c.cfg.Publication.Tables) == 0 {
			return errors.New("online publication creation requires explicit business tables")
		}
		tables := make([]string, len(c.cfg.Publication.Tables))
		for i, t := range c.cfg.Publication.Tables {
			if t.Schema == "cdc_online" {
				return errors.New("online business table configuration includes metadata")
			}
			tables[i] = pgx.Identifier{t.Schema, t.Name}.Sanitize()
		}
		_, err := conn.Exec(ctx, "CREATE PUBLICATION "+pgx.Identifier{c.cfg.Publication.Name}.Sanitize()+" FOR TABLE "+strings.Join(tables, ",")+" WITH (publish='insert,update,delete,truncate')")
		if err != nil {
			return err
		}
	}
	return online.EnsureWatermarkPublication(ctx, conn, c.cfg.Publication.Name, c.cfg.Publication.CreateIfNotExists)
}

// Run performs synchronous fail-closed setup before capture readiness. No slot
// is adopted or recreated; partial fresh initialization requires reconciliation.
// The instance is single-run. Restart by constructing a new instance bound to
// the original durable source metadata and slot.
func (c *OnlineConnector) Run(ctx context.Context) (result error) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("OnlineConnector.Run may be called only once")
	}
	c.started = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.terminal = result; close(c.done); c.mu.Unlock() }()
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := run.Err(); err != nil {
		return err
	}
	var sql *pgx.Conn
	if err := c.query(run, func(ctx context.Context) error {
		parsed, e := pgx.ParseConfig(c.cfg.DSN())
		if e != nil {
			return e
		}
		if e = pq.LimitOnlineFrontend(&parsed.Config, c.cfg.Snapshot.Online.ChunkBytes); e != nil {
			return e
		}
		sql, e = pgx.ConnectConfig(ctx, parsed)
		return e
	}); err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		result = errors.Join(result, sql.Close(cleanup))
	}()
	// Provisioning is deliberately explicit. Use online.Provision/ProvisionSQL as
	// an authorized owner before Run; runtime requires only metadata DML grants.
	if err := c.query(run, func(ctx context.Context) error { return online.ValidateProvisioned(ctx, sql) }); err != nil {
		return err
	}
	var store *online.Store
	var openErr error
	store, openErr = online.OpenStoreWithStartupTimeout(run, c.cfg.DSN(), c.cfg.Slot.Name, c.cfg.Snapshot.Online.QueryTimeout)
	if openErr != nil {
		return openErr
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		result = errors.Join(result, store.Close(cleanup))
	}()
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-store.Done():
			cancel()
		case <-watchDone:
		}
	}()
	if err := c.query(run, func(ctx context.Context) error { return c.setupPublication(ctx, sql) }); err != nil {
		return err
	}
	var source online.SourceIdentity
	if err := c.query(run, func(ctx context.Context) error {
		var e error
		source, e = online.IdentifySource(ctx, c.cfg.ReplicationDSN())
		return e
	}); err != nil {
		return err
	}
	// Validate primary/source identity before creating slot/job state or selecting rows.
	var primary bool
	var system, database string
	var timeline int32
	if err := c.query(run, func(ctx context.Context) error {
		return sql.QueryRow(ctx, `SELECT NOT pg_is_in_recovery(),system_identifier::text,current_database(),('x'||substr(pg_walfile_name(pg_current_wal_lsn()),1,8))::bit(32)::int FROM pg_control_system()`).Scan(&primary, &system, &database, &timeline)
	}); err != nil {
		return fmt.Errorf("online identity query (GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO <cdc_role>): %w", err)
	}
	if !primary || source != (online.SourceIdentity{SystemID: system, Database: database, Timeline: timeline}) {
		return online.ErrBinding
	}
	var schemas []online.TableSchema
	if err := c.query(run, func(ctx context.Context) error {
		var e error
		schemas, e = online.ValidatePublication(ctx, sql, c.cfg.Publication.Name)
		return e
	}); err != nil {
		return err
	}
	selected := make([]online.Table, 0, len(schemas))
	if len(c.cfg.Snapshot.Tables) == 0 {
		for _, s := range schemas {
			selected = append(selected, s.Table)
		}
	} else {
		for _, t := range c.cfg.Snapshot.Tables {
			table := online.Table{Schema: t.Schema, Name: t.Name}
			found := false
			for _, s := range schemas {
				if s.Table == table {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("online snapshot table is not published: %s.%s", t.Schema, t.Name)
			}
			for _, prior := range selected {
				if prior == table {
					return errors.New("online duplicate snapshot table")
				}
			}
			selected = append(selected, table)
		}
	}
	var job online.Job
	err := c.query(run, func(ctx context.Context) error { var e error; job, e = store.LoadJob(ctx); return e })
	if errors.Is(err, pgx.ErrNoRows) {
		var checkpointLSN = job.Checkpoint
		if err = c.query(run, func(ctx context.Context) error {
			var e error
			checkpointLSN, e = online.CreateFreshSlot(ctx, sql, c.cfg.Slot.Name, c.cfg.Slot.CreateIfNotExists)
			return e
		}); err != nil {
			return err
		}
		id, e := online.NewID()
		if e != nil {
			return e
		}
		connector, e := online.NewID()
		if e != nil {
			return e
		}
		binding, e := online.NewID()
		if e != nil {
			return e
		}
		job = online.Job{ID: id, ConnectorID: connector, Slot: c.cfg.Slot.Name, SlotBinding: binding, Source: source, Generation: 1, ConfigFingerprint: c.fingerprint(schemas), Status: "active", Checkpoint: checkpointLSN}
		if err = c.query(run, func(ctx context.Context) error { return store.CreateJob(ctx, job) }); err != nil {
			var persisted online.Job
			lookup := c.query(run, func(ctx context.Context) error { var e error; persisted, e = store.LoadJob(ctx); return e })
			if lookup != nil || persisted != job {
				return errors.Join(err, lookup)
			}
		}
	} else if err != nil {
		return err
	} else {
		var slot online.SlotState
		if err = c.query(run, func(ctx context.Context) error {
			var e error
			slot, e = online.InspectSlot(ctx, sql, c.cfg.Slot.Name)
			return e
		}); err != nil {
			return err
		}
		if err = online.ValidateResume(job, slot, source, c.fingerprint(schemas)); err != nil {
			return err
		}
		if err = c.query(run, func(ctx context.Context) error { var e error; job, e = store.ResumeJob(ctx, job); return e }); err != nil {
			return err
		}
	}
	limits := c.cfg.Snapshot.Online
	reader, err := replication.StartOnlineReaderWithStartupTimeout(run, replication.OnlineReaderOptions{ReplicationDSN: c.cfg.ReplicationDSN(), SQLDSN: c.cfg.DSN(), Publication: c.cfg.Publication.Name, Slot: c.cfg.Slot.Name, StartLSN: job.Checkpoint, ProtoVersion: 1, Source: source, Limits: replication.OnlineReaderLimits{TransactionBytes: limits.TransactionBytes, QueueBytes: limits.QueueBytes, FeedbackInterval: time.Second, VisibilityTimeout: limits.QueryTimeout, PollInterval: 5 * time.Millisecond}}, limits.QueryTimeout)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		result = errors.Join(result, reader.Close(cleanup))
		if e := reader.Err(); e != nil && !errors.Is(e, context.Canceled) {
			result = errors.Join(result, e)
		}
		if e := store.Err(); errors.Is(e, online.ErrOwnerLost) {
			result = errors.Join(result, e)
		}
	}()
	go func() {
		select {
		case <-reader.Done():
			cancel()
		case <-watchDone:
		}
	}()
	c.mu.Lock()
	c.reader = reader
	c.mu.Unlock()
	coordinator := online.Coordinator{Store: store, SQL: sql, Reader: reader, Job: job, Schemas: schemas, Handler: c.handler, Options: online.CoordinatorOptions{Publication: c.cfg.Publication.Name, Selected: selected, ChunkRows: int(c.cfg.Snapshot.ChunkSize), ChunkBytes: limits.ChunkBytes, QueryTimeout: limits.QueryTimeout, DeliveryTimeout: limits.DeliveryTimeout, CheckpointInterval: limits.CheckpointInterval, MarkerInterval: limits.MarkerInterval, SnapshotComplete: func() { close(c.snapshot) }, Observer: c.observer, Counters: &c.counters}}
	if c.observer != nil {
		call, stop := context.WithTimeout(run, limits.DeliveryTimeout)
		err = c.observer(call, online.Observation{Phase: online.CaptureStarted, JobID: job.ID, Generation: job.Generation, EndLSN: job.Checkpoint})
		if err == nil {
			err = call.Err()
		}
		stop()
		if err != nil {
			return err
		}
	}
	close(c.capture)
	return coordinator.Run(run)
}
