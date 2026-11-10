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
	if err := c.beginOnlineRun(); err != nil {
		return err
	}
	defer func() { c.finishOnlineRun(result) }()
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := run.Err(); err != nil {
		return err
	}
	sql, err := c.openOnlineSQL(run)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, closeOnlineComponent(run, sql.Close)) }()
	store, err := c.openOnlineStore(run, sql)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, closeOnlineComponent(run, store.Close)) }()
	watchDone := make(chan struct{})
	defer close(watchDone)
	go watchOnlineDone(store.Done(), watchDone, cancel)
	coordinator, err := c.prepareOnlineCapture(run, sql, store)
	if err != nil {
		return err
	}
	reader := coordinator.Reader
	defer func() { result = errors.Join(result, finishOnlineReader(run, reader, store)) }()
	go watchOnlineDone(reader.Done(), watchDone, cancel)
	c.mu.Lock()
	c.reader = reader
	c.mu.Unlock()
	if err := c.observeOnlineCapture(run, coordinator.Job); err != nil {
		return err
	}
	close(c.capture)
	return coordinator.Run(run)
}

func (c *OnlineConnector) beginOnlineRun() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return errors.New("OnlineConnector.Run may be called only once")
	}
	c.started = true
	return nil
}

func (c *OnlineConnector) finishOnlineRun(result error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminal = result
	close(c.done)
}

func closeOnlineComponent(ctx context.Context, closeComponent func(context.Context) error) error {
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	return closeComponent(cleanup)
}

func finishOnlineReader(ctx context.Context, reader *replication.OnlineReader, store *online.Store) error {
	result := closeOnlineComponent(ctx, reader.Close)
	if e := reader.Err(); e != nil && !errors.Is(e, context.Canceled) {
		result = errors.Join(result, e)
	}
	if e := store.Err(); errors.Is(e, online.ErrOwnerLost) {
		result = errors.Join(result, e)
	}
	return result
}

func watchOnlineDone(done, stop <-chan struct{}, cancel context.CancelFunc) {
	select {
	case <-done:
		cancel()
	case <-stop:
	}
}

func (c *OnlineConnector) openOnlineSQL(ctx context.Context) (*pgx.Conn, error) {
	var sql *pgx.Conn
	err := c.query(ctx, func(ctx context.Context) error {
		parsed, err := pgx.ParseConfig(c.cfg.DSN())
		if err != nil {
			return err
		}
		if err = pq.LimitOnlineFrontend(&parsed.Config, c.cfg.Snapshot.Online.ChunkBytes); err != nil {
			return err
		}
		sql, err = pgx.ConnectConfig(ctx, parsed)
		return err
	})
	return sql, err
}

func (c *OnlineConnector) openOnlineStore(ctx context.Context, sql *pgx.Conn) (*online.Store, error) {
	// Provisioning remains explicit and precedes advisory owner acquisition.
	if err := c.query(ctx, func(ctx context.Context) error { return online.ValidateProvisioned(ctx, sql) }); err != nil {
		return nil, err
	}
	return online.OpenStoreWithStartupTimeout(ctx, c.cfg.DSN(), c.cfg.Slot.Name, c.cfg.Snapshot.Online.QueryTimeout)
}

func (c *OnlineConnector) prepareOnlineCapture(ctx context.Context, sql *pgx.Conn, store *online.Store) (*online.Coordinator, error) {
	if err := c.query(ctx, func(ctx context.Context) error { return c.setupPublication(ctx, sql) }); err != nil {
		return nil, err
	}
	source, schemas, selected, err := c.onlineCatalog(ctx, sql)
	if err != nil {
		return nil, err
	}
	job, err := c.onlineJob(ctx, store, sql, source, schemas)
	if err != nil {
		return nil, err
	}
	limits := c.cfg.Snapshot.Online
	reader, err := replication.StartOnlineReaderWithStartupTimeout(ctx, replication.OnlineReaderOptions{ReplicationDSN: c.cfg.ReplicationDSN(), SQLDSN: c.cfg.DSN(), Publication: c.cfg.Publication.Name, Slot: c.cfg.Slot.Name, StartLSN: job.Checkpoint, ProtoVersion: 1, Source: source, Limits: replication.OnlineReaderLimits{TransactionBytes: limits.TransactionBytes, QueueBytes: limits.QueueBytes, FeedbackInterval: time.Second, VisibilityTimeout: limits.QueryTimeout, PollInterval: 5 * time.Millisecond}}, limits.QueryTimeout)
	if err != nil {
		return nil, err
	}
	coordinator := online.Coordinator{Store: store, SQL: sql, Reader: reader, Job: job, Schemas: schemas, Handler: c.handler, Options: online.CoordinatorOptions{Publication: c.cfg.Publication.Name, Selected: selected, ChunkRows: int(c.cfg.Snapshot.ChunkSize), ChunkBytes: limits.ChunkBytes, QueryTimeout: limits.QueryTimeout, DeliveryTimeout: limits.DeliveryTimeout, CheckpointInterval: limits.CheckpointInterval, MarkerInterval: limits.MarkerInterval, SnapshotComplete: func() { close(c.snapshot) }, Observer: c.observer, Counters: &c.counters}}
	return &coordinator, nil
}

func (c *OnlineConnector) observeOnlineCapture(ctx context.Context, job online.Job) error {
	limits := c.cfg.Snapshot.Online
	var err error
	if c.observer != nil {
		call, stop := context.WithTimeout(ctx, limits.DeliveryTimeout)
		err = c.observer(call, online.Observation{Phase: online.CaptureStarted, JobID: job.ID, Generation: job.Generation, EndLSN: job.Checkpoint})
		if err == nil {
			err = call.Err()
		}
		stop()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *OnlineConnector) onlineCatalog(ctx context.Context, sql *pgx.Conn) (online.SourceIdentity, []online.TableSchema, []online.Table, error) {
	var source online.SourceIdentity
	if err := c.query(ctx, func(ctx context.Context) error {
		var e error
		source, e = online.IdentifySource(ctx, c.cfg.ReplicationDSN())
		return e
	}); err != nil {
		return online.SourceIdentity{}, nil, nil, err
	}
	// Validate primary/source identity before creating slot/job state or selecting rows.
	var primary bool
	var system, database string
	var timeline int32
	if err := c.query(ctx, func(ctx context.Context) error {
		return sql.QueryRow(ctx, `SELECT NOT pg_is_in_recovery(),system_identifier::text,current_database(),('x'||substr(pg_walfile_name(pg_current_wal_lsn()),1,8))::bit(32)::int FROM pg_control_system()`).Scan(&primary, &system, &database, &timeline)
	}); err != nil {
		return online.SourceIdentity{}, nil, nil, fmt.Errorf("online identity query (GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO <cdc_role>): %w", err)
	}
	if !primary || source != (online.SourceIdentity{SystemID: system, Database: database, Timeline: timeline}) {
		return online.SourceIdentity{}, nil, nil, online.ErrBinding
	}
	var schemas []online.TableSchema
	if err := c.query(ctx, func(ctx context.Context) error {
		var e error
		schemas, e = online.ValidatePublication(ctx, sql, c.cfg.Publication.Name)
		return e
	}); err != nil {
		return online.SourceIdentity{}, nil, nil, err
	}
	selected, err := c.onlineSelectedTables(schemas)
	return source, schemas, selected, err
}

func (c *OnlineConnector) onlineSelectedTables(schemas []online.TableSchema) ([]online.Table, error) {
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
				return nil, fmt.Errorf("online snapshot table is not published: %s.%s", t.Schema, t.Name)
			}
			for _, prior := range selected {
				if prior == table {
					return nil, errors.New("online duplicate snapshot table")
				}
			}
			selected = append(selected, table)
		}
	}
	return selected, nil
}

func (c *OnlineConnector) onlineJob(ctx context.Context, store *online.Store, sql *pgx.Conn, source online.SourceIdentity, schemas []online.TableSchema) (online.Job, error) {
	var job online.Job
	err := c.query(ctx, func(ctx context.Context) error { var e error; job, e = store.LoadJob(ctx); return e })
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return c.createOnlineJob(ctx, store, sql, source, schemas)
	case err != nil:
		return online.Job{}, err
	default:
		return c.resumeOnlineJob(ctx, store, sql, job, source, schemas)
	}
}

func (c *OnlineConnector) createOnlineJob(ctx context.Context, store *online.Store, sql *pgx.Conn, source online.SourceIdentity, schemas []online.TableSchema) (online.Job, error) {
	var job online.Job
	var err error
	var checkpointLSN = job.Checkpoint
	if err = c.query(ctx, func(ctx context.Context) error {
		var e error
		checkpointLSN, e = online.CreateFreshSlot(ctx, sql, c.cfg.Slot.Name, c.cfg.Slot.CreateIfNotExists)
		return e
	}); err != nil {
		return online.Job{}, err
	}
	id, e := online.NewID()
	if e != nil {
		return online.Job{}, e
	}
	connector, e := online.NewID()
	if e != nil {
		return online.Job{}, e
	}
	binding, e := online.NewID()
	if e != nil {
		return online.Job{}, e
	}
	job = online.Job{ID: id, ConnectorID: connector, Slot: c.cfg.Slot.Name, SlotBinding: binding, Source: source, Generation: 1, ConfigFingerprint: c.fingerprint(schemas), Status: "active", Checkpoint: checkpointLSN}
	if err = c.query(ctx, func(ctx context.Context) error { return store.CreateJob(ctx, job) }); err != nil {
		var persisted online.Job
		lookup := c.query(ctx, func(ctx context.Context) error {
			var e error
			persisted, e = store.LoadJob(ctx)
			return e
		})
		if lookup != nil || persisted != job {
			return online.Job{}, errors.Join(err, lookup)
		}
	}
	return job, nil
}

func (c *OnlineConnector) resumeOnlineJob(ctx context.Context, store *online.Store, sql *pgx.Conn, job online.Job, source online.SourceIdentity, schemas []online.TableSchema) (online.Job, error) {
	var err error
	var slot online.SlotState
	if err = c.query(ctx, func(ctx context.Context) error {
		var e error
		slot, e = online.InspectSlot(ctx, sql, c.cfg.Slot.Name)
		return e
	}); err != nil {
		return online.Job{}, err
	}
	if err = online.ValidateResume(job, slot, source, c.fingerprint(schemas)); err != nil {
		return online.Job{}, err
	}
	if err = c.query(ctx, func(ctx context.Context) error { var e error; job, e = store.ResumeJob(ctx, job); return e }); err != nil {
		return online.Job{}, err
	}
	return job, nil
}
