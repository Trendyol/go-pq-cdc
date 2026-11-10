package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	libpq "github.com/lib/pq"
)

// OnlineSourceIdentity binds both non-reconnecting connections to the source
// recorded by the job. A fresh job obtains this from IDENTIFY_SYSTEM before
// persisting its slot binding. No existing slot or source is adopted here.
type OnlineSourceIdentity struct {
	SystemID string
	Database string
	Timeline int32
}

type OnlineReaderLimits struct {
	TransactionBytes  int64
	QueueBytes        int64
	FeedbackInterval  time.Duration // choose below source wal_sender_timeout
	VisibilityTimeout time.Duration
	PollInterval      time.Duration
}

// OnlineReaderOptions deliberately requires explicit protocol, start position,
// identity and limits. StartLSN is a caller-certified durable checkpoint (or the
// freshly owned slot's consistent start point), and is the initial safe ACK.
// DSNs must be direct endpoints for the same primary/database, not poolers.
type OnlineReaderOptions struct {
	ReplicationDSN string
	SQLDSN         string
	Publication    string
	Slot           string
	StartLSN       pq.LSN
	ProtoVersion   int
	Source         OnlineSourceIdentity
	Limits         OnlineReaderLimits
}

func (o OnlineReaderOptions) Validate() error {
	if o.ProtoVersion != 1 {
		return errors.New("online reader requires protocol 1; protocol 2 is not downgraded")
	}
	if o.StartLSN == 0 {
		return errors.New("online reader requires an explicit nonzero durable start LSN")
	}
	if o.Source.SystemID == "" || o.Source.Database == "" || o.Source.Timeline <= 0 {
		return errors.New("online reader requires source systemID, database and timeline")
	}
	for _, s := range []string{o.ReplicationDSN, o.SQLDSN, o.Publication, o.Slot} {
		if strings.TrimSpace(s) == "" || strings.ContainsRune(s, 0) {
			return errors.New("online reader requires nonempty DSNs/publication/slot without NUL")
		}
	}
	if o.Limits.TransactionBytes < 47 {
		return errors.New("online transaction byte limit must admit BEGIN and COMMIT (47 bytes)")
	}
	if _, err := pq.OnlineBodyLimit(o.Limits.QueueBytes); err != nil {
		return err
	}
	if o.Limits.QueueBytes < o.Limits.TransactionBytes {
		return errors.New("online queue byte limit must admit one maximum transaction")
	}
	if o.Limits.FeedbackInterval <= 0 || o.Limits.VisibilityTimeout <= 0 || o.Limits.PollInterval <= 0 {
		return errors.New("online feedback/visibility/poll durations must be positive")
	}
	return nil
}

type onlineTransport interface {
	receive(context.Context) (pgproto3.BackendMessage, error)
	send(context.Context, pq.LSN) error
	close(context.Context) error
}
type onlineGate interface {
	wait(context.Context, uint32) error
	close(context.Context) error
}

type onlinePGTransport struct{ conn *onlineConnected }

func (t onlinePGTransport) receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	return t.conn.ReceiveMessage(ctx)
}
func (t onlinePGTransport) close(ctx context.Context) error { return t.conn.Close(ctx) }
func (t onlinePGTransport) send(ctx context.Context, lsn pq.LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b := make([]byte, 34)
	b[0] = StandbyStatusUpdateByteID
	for _, offset := range []int{1, 9, 17} {
		binary.BigEndian.PutUint64(b[offset:], uint64(lsn))
	}
	binary.BigEndian.PutUint64(b[25:], uint64(time.Now().UnixMicro()-microSecFromUnixEpochToY2K))
	return onlineWrite(ctx, t.conn.Conn(), func() error {
		t.conn.Frontend().Send(&pgproto3.CopyData{Data: b})
		return t.conn.Frontend().Flush()
	})
}

// Frontend.Flush has no context parameter. The sole I/O owner brackets writes
// with a socket deadline and waits for its cancellation watcher before clearing
// the deadline; no deadline toggle can race a subsequent ReceiveMessage.
func onlineWrite(ctx context.Context, conn net.Conn, write func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	watchDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.SetWriteDeadline(time.Now()); close(watchDone) })
	err := write()
	if !stop() {
		<-watchDone
	}
	resetErr := conn.SetWriteDeadline(time.Time{})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(err, resetErr)
}

// OnlineReaderStats reports bounded encoded-byte and safe-feedback counters.
type OnlineReaderStats struct {
	PeakTransactionEncodedBytes, PeakQueueAndAssemblyEncodedBytes, PeakRelationEncodedBytes, BufferedEncodedBytes int64
	SafeACK                                                                                                       pq.LSN
	FeedbackWrites                                                                                                uint64
	LastFeedback                                                                                                  time.Time
}

// OnlineReader owns replication I/O and the primary guard. Next waits for
// COMMIT and primary visibility; there is no automatic ACK, retry or reconnect.
// Callers own returned immutable envelopes and bounded durable-delivery work.
type OnlineReader struct {
	options         OnlineReaderOptions
	transport       onlineTransport
	gate            onlineGate
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	wake            chan struct{}
	capacityChanged chan struct{}
	nextOwner       chan struct{} // context-aware sole SQL guard owner, including cleanup
	mu              sync.Mutex
	queue           []*OnlineCommittedEnvelope
	buffered        int64 // queued + removed-but-still-gating envelope bytes
	safeACK         pq.LSN
	consumed        pq.LSN
	terminal        error
	stats           OnlineReaderStats
}

// StartOnlineReader returns only after identity/PG16/primary checks and
// CopyBoth readiness. It never creates a slot/publication or source metadata.
// pg_control_system() requires EXECUTE; failures name the minimum grant.
func StartOnlineReader(ctx context.Context, o OnlineReaderOptions) (*OnlineReader, error) {
	return StartOnlineReaderWithStartupTimeout(ctx, o, 5*time.Second)
}

// StartOnlineReaderWithStartupTimeout bounds connection acquisition, identity,
// guard SQL and CopyBoth negotiation together. ctx owns the successful reader's
// lifetime; canceling/expiring the derived startup context cannot stop that owner.
func StartOnlineReaderWithStartupTimeout(ctx context.Context, o OnlineReaderOptions, timeout time.Duration) (*OnlineReader, error) {
	if timeout <= 0 {
		return nil, errors.New("online reader startup timeout must be positive")
	}
	startup, cancelStartup := context.WithTimeout(ctx, timeout)
	defer cancelStartup()
	if err := o.Validate(); err != nil {
		return nil, err
	}
	conn, err := openOnlineConnection(startup, o.ReplicationDSN, o.Limits.QueueBytes)
	if err != nil {
		return nil, fmt.Errorf("online replication connect: %w", err)
	}
	if err = checkOnlineIdentity(startup, conn, o.Source); err != nil {
		return nil, errors.Join(err, closeOnlineResource(ctx, conn.Close))
	}
	gate, err := openOnlineGuard(startup, o)
	if err != nil {
		return nil, errors.Join(err, closeOnlineResource(ctx, conn.Close))
	}
	if err = negotiateOnlineReplication(startup, conn, o); err != nil {
		return nil, errors.Join(fmt.Errorf("online START_REPLICATION: %w", err), closeOnlineResource(ctx, gate.close), closeOnlineResource(ctx, conn.Close))
	}
	return newOnlineReader(ctx, o, onlinePGTransport{conn}, gate), nil
}

func closeOnlineResource(ctx context.Context, closeResource func(context.Context) error) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return closeResource(cleanup)
}

func openOnlineConnection(ctx context.Context, dsn string, budget int64) (*onlineConnected, error) {
	config, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if err = pq.LimitOnlineFrontend(config, budget); err != nil {
		return nil, err
	}
	raw, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return &onlineConnected{raw}, nil
}

func checkOnlineIdentity(ctx context.Context, conn *onlineConnected, expected OnlineSourceIdentity) error {
	source, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("online IDENTIFY_SYSTEM: %w", err)
	}
	if source.SystemID != expected.SystemID || source.Database != expected.Database || source.Timeline != expected.Timeline {
		return errors.New("online replication source identity mismatch")
	}
	return nil
}

func openOnlineGuard(ctx context.Context, options OnlineReaderOptions) (*onlineVisibilityGate, error) {
	conn, err := openOnlineConnection(ctx, options.SQLDSN, options.Limits.QueueBytes)
	if err != nil {
		return nil, fmt.Errorf("online primary guard connect: %w", err)
	}
	gate, err := openOnlineVisibilityGate(ctx, conn, options)
	if err != nil {
		return nil, errors.Join(err, closeOnlineResource(ctx, conn.Close))
	}
	return gate, nil
}

func negotiateOnlineReplication(ctx context.Context, conn *onlineConnected, options OnlineReaderOptions) error {
	replication := New(conn)
	// Identifier list inside a SQL literal, never raw publication SQL.
	publication := strings.ReplaceAll(libpq.QuoteIdentifier(options.Publication), "'", "''")
	err := onlineWrite(ctx, conn.Conn(), func() error { return replication.Start(publication, options.Slot, options.StartLSN, 1) })
	if err != nil {
		return err
	}
	return replication.Test(ctx)
}

type onlineConnected struct{ *pgconn.PgConn }

func (*onlineConnected) Connect(context.Context) error {
	return errors.New("online connection never reconnects")
}

func newOnlineReader(ctx context.Context, o OnlineReaderOptions, t onlineTransport, g onlineGate) *OnlineReader {
	runCtx, cancel := context.WithCancel(ctx)
	r := &OnlineReader{options: o, transport: t, gate: g, ctx: runCtx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1), capacityChanged: make(chan struct{}, 1), nextOwner: make(chan struct{}, 1), safeACK: o.StartLSN, consumed: o.StartLSN}
	r.nextOwner <- struct{}{}
	go r.run()
	return r
}
func (r *OnlineReader) notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
	select {
	case r.capacityChanged <- struct{}{}:
	default:
	}
}
func (r *OnlineReader) fail(err error) {
	r.mu.Lock()
	if r.terminal == nil {
		r.terminal = err
	}
	r.mu.Unlock()
	r.cancel()
	r.notify()
}
func (r *OnlineReader) Stats() OnlineReaderStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.stats
	s.BufferedEncodedBytes = r.buffered
	s.SafeACK = r.safeACK
	return s
}
func (r *OnlineReader) Err() error            { r.mu.Lock(); defer r.mu.Unlock(); return r.terminal }
func (r *OnlineReader) Done() <-chan struct{} { return r.done }

// BufferedBytes counts queued/in-gate encoded bytes, not current assembly,
// cached relations, caller-retained envelopes or measured heap overhead.
func (r *OnlineReader) BufferedBytes() int64 { r.mu.Lock(); defer r.mu.Unlock(); return r.buffered }

// AdvanceSafeACK accepts only monotonic caller-proven progress at/below the
// visibility-gated consumed frontier. It does not imply target completion;
// the caller must persist its durable checkpoint and enforce the LOW ceiling
// first. Receiving WAL/keepalives/COMMIT or returning Next never auto-ACKs.
func (r *OnlineReader) AdvanceSafeACK(lsn pq.LSN) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return r.terminal
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if lsn < r.safeACK || lsn > r.consumed {
		return errors.New("online safe ACK must be monotonic and at/below consumed visibility frontier")
	}
	r.safeACK = lsn
	return nil
}

// Next blocks until one committed envelope passes the primary visibility gate.
// Any cancellation/error is terminal for this reader, including Next's context;
// callers restart through the job/slot fencing protocol, never by retrying SQL.
func (r *OnlineReader) Next(ctx context.Context) (*OnlineCommittedEnvelope, error) {
	if err := r.acquireNext(ctx); err != nil {
		return nil, err
	}
	defer func() { r.nextOwner <- struct{}{} }()
	nextCtx, cancel := context.WithCancel(r.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			r.fail(err)
			return nil, err
		}
		env, err := r.takeEnvelope()
		if err != nil {
			r.fail(err)
			return nil, err
		}
		if env != nil {
			return r.gateEnvelope(ctx, nextCtx, env)
		}
		select {
		case <-nextCtx.Done():
			err := ctx.Err()
			if err == nil {
				err = r.ctx.Err()
			}
			if err == nil {
				err = nextCtx.Err()
			}
			r.fail(err)
			return nil, err
		case <-r.wake:
		}
	}
}

func (r *OnlineReader) acquireNext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		r.fail(ctx.Err())
		return ctx.Err()
	case <-r.ctx.Done():
		if err := r.Err(); err != nil {
			return err
		}
		return r.ctx.Err()
	case <-r.nextOwner:
		return nil
	}
}

// Removed envelopes keep their queue charge until visibility certification.
func (r *OnlineReader) takeEnvelope() (*OnlineCommittedEnvelope, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != nil {
		return nil, r.terminal
	}
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if len(r.queue) == 0 {
		return nil, nil
	}
	env := r.queue[0]
	r.queue[0] = nil
	r.queue = r.queue[1:]
	return env, nil
}

func (r *OnlineReader) gateEnvelope(ctx, nextCtx context.Context, env *OnlineCommittedEnvelope) (*OnlineCommittedEnvelope, error) {
	err := r.gate.wait(nextCtx, env.XID)
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		r.fail(err)
		return nil, err
	}
	r.mu.Lock()
	if r.terminal != nil {
		err = r.terminal
	} else {
		err = r.ctx.Err()
	}
	if err == nil {
		r.buffered -= env.EncodedBytes
		r.consumed = env.EndLSN
	}
	r.mu.Unlock()
	if err != nil {
		r.fail(err)
		return nil, err
	}
	r.notify()
	return env, nil
}

// Close cancels both owners and waits for connection closure. It never flushes
// a new ACK. A Close deadline limits the caller's wait, not an arbitrary sink
// callback (there are no callbacks here). Done closes after both owners exit.
func (r *OnlineReader) Close(ctx context.Context) error {
	r.cancel()
	r.notify()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *OnlineReader) sendFeedback() error {
	r.mu.Lock()
	ack := r.safeACK
	terminal := r.terminal
	r.mu.Unlock()
	if terminal != nil {
		return terminal
	}
	sendCtx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	err := r.transport.send(sendCtx, ack)
	if err == nil {
		r.mu.Lock()
		r.stats.FeedbackWrites++
		r.stats.LastFeedback = time.Now()
		r.mu.Unlock()
	}
	return err
}
func (r *OnlineReader) run() {
	err := r.readLoop()
	if err == nil {
		err = context.Canceled
	}
	r.fail(err)
	// Release all volatile buffers on terminal failure; none can be consumed.
	r.mu.Lock()
	r.queue = nil
	r.buffered = 0
	r.mu.Unlock()
	ioErr := closeOnlineResource(r.ctx, r.transport.close)
	<-r.nextOwner
	gateErr := closeOnlineResource(r.ctx, r.gate.close)
	r.nextOwner <- struct{}{}
	if ioErr != nil || gateErr != nil {
		r.mu.Lock()
		r.terminal = errors.Join(r.terminal, ioErr, gateErr)
		r.mu.Unlock()
	}
	close(r.done)
}
func (r *OnlineReader) readLoop() error {
	a := newOnlineAssembler(r.options.Limits.TransactionBytes, r.options.Limits.QueueBytes)
	nextFeedback := time.Now()
	lastEnd := r.options.StartLSN
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(nextFeedback) {
			if err := r.sendFeedback(); err != nil {
				return err
			}
			nextFeedback = time.Now().Add(r.options.Limits.FeedbackInterval)
		}
		// Reserve room for a maximum transaction before reading its BEGIN. This
		// keeps queue + assembly <= QueueBytes, with no unbounded pending-send queue.
		r.mu.Lock()
		room := r.options.Limits.QueueBytes - r.buffered
		r.mu.Unlock()
		if !a.inTransaction() && room < r.options.Limits.TransactionBytes {
			timer := time.NewTimer(time.Until(nextFeedback))
			select {
			case <-r.ctx.Done():
				timer.Stop()
				return r.ctx.Err()
			case <-r.capacityChanged:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		deadline := time.Now().Add(100 * time.Millisecond)
		if nextFeedback.Before(deadline) {
			deadline = nextFeedback
		}
		receiveCtx, cancel := context.WithDeadline(r.ctx, deadline)
		raw, err := r.transport.receive(receiveCtx)
		cancel()
		if err != nil {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}
			if pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("online replication receive: %w", err)
		}
		lastEnd, nextFeedback, err = r.dispatchOnlineMessage(raw, a, lastEnd, nextFeedback)
		if err != nil {
			return err
		}
	}
}

func (r *OnlineReader) handleOnlineKeepalive(data []byte) (bool, error) {
	keepalive, err := format.NewPrimaryKeepaliveMessage(data[1:])
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrOnlineProtocol, err)
	}
	if data[17] > 1 {
		return false, fmt.Errorf("%w: invalid keepalive reply flag", ErrOnlineProtocol)
	}
	if !keepalive.ReplyRequested {
		return false, nil
	}
	if err = r.sendFeedback(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *OnlineReader) acceptOnlineXLog(a *onlineAssembler, data []byte, lastEnd pq.LSN) (pq.LSN, error) {
	xld, err := ParseXLogData(data[1:])
	if err != nil {
		return lastEnd, fmt.Errorf("%w: %w", ErrOnlineProtocol, err)
	}
	env, err := a.accept(xld)
	if err != nil {
		return lastEnd, err
	}
	r.mu.Lock()
	active := int64(0)
	if a.tx != nil {
		active = a.tx.EncodedBytes
	}
	size := active
	if env != nil {
		size = env.EncodedBytes
	}
	r.stats.PeakTransactionEncodedBytes = max(r.stats.PeakTransactionEncodedBytes, size)
	r.stats.PeakQueueAndAssemblyEncodedBytes = max(r.stats.PeakQueueAndAssemblyEncodedBytes, r.buffered+size)
	r.stats.PeakRelationEncodedBytes = max(r.stats.PeakRelationEncodedBytes, a.metadataBytes)
	r.mu.Unlock()
	if env != nil {
		if env.EndLSN <= lastEnd {
			return lastEnd, fmt.Errorf("%w: non-increasing transaction end LSN", ErrOnlineProtocol)
		}
		lastEnd = env.EndLSN
		r.mu.Lock()
		r.queue = append(r.queue, env)
		r.buffered += env.EncodedBytes
		r.mu.Unlock()
		r.notify()
	}
	return lastEnd, nil
}

func (r *OnlineReader) dispatchOnlineMessage(raw pgproto3.BackendMessage, a *onlineAssembler, lastEnd pq.LSN, nextFeedback time.Time) (pq.LSN, time.Time, error) {
	switch m := raw.(type) {
	case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		return lastEnd, nextFeedback, nil
	case *pgproto3.ErrorResponse:
		return lastEnd, nextFeedback, pgconn.ErrorResponseToPgError(m)
	case *pgproto3.CopyData:
		if len(m.Data) == 0 {
			return lastEnd, nextFeedback, fmt.Errorf("%w: empty CopyData", ErrOnlineProtocol)
		}
		switch m.Data[0] {
		case 'k':
			replied, err := r.handleOnlineKeepalive(m.Data)
			if err != nil {
				return lastEnd, nextFeedback, err
			}
			if replied {
				nextFeedback = time.Now().Add(r.options.Limits.FeedbackInterval)
			}
		case 'w':
			var err error
			lastEnd, err = r.acceptOnlineXLog(a, m.Data, lastEnd)
			if err != nil {
				return lastEnd, nextFeedback, err
			}
		default:
			return lastEnd, nextFeedback, fmt.Errorf("%w: unknown CopyData kind", ErrOnlineProtocol)
		}
	default:
		return lastEnd, nextFeedback, fmt.Errorf("%w: unexpected backend message %T", ErrOnlineProtocol, raw)
	}
	return lastEnd, nextFeedback, nil
}
