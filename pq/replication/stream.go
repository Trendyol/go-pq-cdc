package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/internal/slice"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/avast/retry-go/v4"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrorSlotInUse    = errors.New("replication slot in use")
	ErrorNotConnected = errors.New("stream is not connected")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerContext struct {
	Message any
	Ack     func() error
}

type ListenerFunc func(ctx *ListenerContext)

type Message struct {
	message  any
	walStart int64
}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	OpenFromSnapshotLSN()
}

type stream struct {
	conn                pq.Connection
	metric              metric.Metric
	system              *pq.IdentifySystemResult
	relation            map[uint32]*format.Relation
	messageCH           chan *Message
	listenerFunc        ListenerFunc
	sinkEnd             chan struct{}
	mu                  *sync.RWMutex
	config              config.Config
	lastXLogPos         pq.LSN
	snapshotLSN         pq.LSN
	openFromSnapshotLSN bool
	closed              atomic.Bool
}

func NewStream(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		// lastXLogPos:0 is not magical, 0 means, create replication starts with confirmed_flush_lsn
		// https://github.com/postgres/postgres/blob/master/src/include/access/xlogdefs.h#L28
		// https://github.com/postgres/postgres/blob/master/src/backend/replication/logical/logical.c#L540
		lastXLogPos: 0,
		sinkEnd:     make(chan struct{}, 1),
		mu:          &sync.RWMutex{},
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		s.sinkEnd <- struct{}{}

		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	go s.sink(ctx)

	go s.process(ctx)

	logger.Info("cdc stream started")

	return nil
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}
		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn, s.config.Slot.ProtoVersion); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())
	} else {
		logger.Info("replication started from confirmed_flush_lsn", "slot", s.config.Slot.Name)
	}

	return nil
}

// messageBuffer manages a one-message look-ahead buffer.
//
// The last DML message in each transaction is held back so its WAL position
// can be rewritten to the transaction-end LSN (from COMMIT / STREAM COMMIT).
// All preceding messages are emitted immediately with their original position.
// This keeps memory usage O(1) regardless of transaction size.
type messageBuffer struct {
	pending *Message
	outCh   chan<- *Message
}

// flush emits the pending message (if any) with its original WAL position.
func (b *messageBuffer) flush() {
	if b.pending != nil {
		b.outCh <- b.pending
		b.pending = nil
	}
}

// flushWithLSN emits the pending message (if any), rewriting its WAL position
// to the given transaction-end LSN. Used at COMMIT.
func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		b.outCh <- &Message{
			message:  b.pending.message,
			walStart: int64(lsn),
		}
		b.pending = nil
	}
}

// discard drops the pending message without emitting.
// Used at BEGIN to reset state.
func (b *messageBuffer) discard() {
	b.pending = nil
}

// buffer stores a new DML message, first flushing any previously pending one.
func (b *messageBuffer) buffer(msg *Message) {
	b.flush()
	b.pending = msg
}

// streamTxBuffer accumulates messages from streaming in-progress transactions.
//
// PostgreSQL streams large transactions in chunks (STREAM START / STREAM STOP)
// before the transaction is committed. Chunks from different transactions may
// be interleaved (e.g. TX-A chunk, TX-B chunk, TX-A chunk, …), so messages
// are stored per-XID in a map.
//
// Messages must NOT be delivered to the consumer until STREAM COMMIT arrives,
// because the transaction may still be rolled back (STREAM ABORT). This mirrors
// how PostgreSQL's own logical replication worker handles streaming: it writes
// to temporary storage and only applies on commit.
type streamTxBuffer struct {
	txns      map[uint32][]*Message
	activeXid uint32
	streaming bool
}

// startTx marks the beginning of a streaming chunk for the given XID.
func (s *streamTxBuffer) startTx(xid uint32) {
	if s.txns == nil {
		s.txns = make(map[uint32][]*Message)
	}
	s.activeXid = xid
	s.streaming = true
}

// append adds a message to the currently active streaming transaction.
func (s *streamTxBuffer) append(msg *Message) {
	if msg != nil {
		s.txns[s.activeXid] = append(s.txns[s.activeXid], msg)
	}
}

// stopTx marks the end of the current streaming chunk.
func (s *streamTxBuffer) stopTx() {
	s.streaming = false
}

// flushTx emits every accumulated message for the given XID through outCh.
// The last message's WAL position is rewritten to the transaction-end LSN.
func (s *streamTxBuffer) flushTx(xid uint32, outCh chan<- *Message, endLSN pq.LSN) {
	s.streaming = false
	msgs := s.txns[xid]
	n := len(msgs)
	for i, msg := range msgs {
		if i == n-1 {
			outCh <- &Message{
				message:  msg.message,
				walStart: int64(endLSN),
			}
		} else {
			outCh <- msg
		}
	}
	delete(s.txns, xid)
}

// discardTx drops all accumulated messages for the given XID without emitting.
func (s *streamTxBuffer) discardTx(xid uint32) {
	s.streaming = false
	delete(s.txns, xid)
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	buf := &messageBuffer{outCh: s.messageCH}
	streamBuf := &streamTxBuffer{}
	corrupted := s.sinkLoop(ctx, buf, streamBuf)

	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {
		s.Close(ctx)
		if corrupted {
			panic("corrupted connection")
		}
	}
}

// sinkLoop reads raw replication messages and dispatches them until the
// connection is closed or a fatal error occurs. It returns true when the
// connection is in a corrupted state and the caller should panic.
func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer, streamBuf *streamTxBuffer) (corrupted bool) {
	for {
		msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(300*time.Millisecond))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				return false
			}
			if pgconn.Timeout(err) {
				if s.LoadXLogPos() > 0 {
					if err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos())); err != nil {
						logger.Error("send stand by status update", "error", err)
						return true
					}
					logger.Debug("send stand by status update")
				}
				continue
			}
			logger.Error("receive message error", "error", err)
			return true
		}

		copyData, ok := s.extractCopyData(rawMsg)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			if err := s.handleKeepalive(ctx, copyData.Data[1:]); err != nil {
				return true
			}
		case message.XLogDataByteID:
			s.handleXLogData(copyData.Data[1:], buf, streamBuf)
		}
	}
}

// extractCopyData validates a raw backend message. It returns the CopyData
// payload and true on success, or (nil, false) for protocol-level errors and
// unexpected message types which are logged and skipped.
func (s *stream) extractCopyData(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, bool) {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		res, _ := errMsg.MarshalJSON()
		logger.Error("receive postgres wal error: " + string(res))
		return nil, false
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
		return nil, false
	}

	return msg, true
}

// handleKeepalive processes a primary keepalive message, updating the WAL
// position and responding with a standby status update when requested.
// A non-nil return signals a corrupted connection.
func (s *stream) handleKeepalive(ctx context.Context, data []byte) error {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		logger.Error("decode primary keepalive message", "error", err)
		return nil // non-fatal, skip
	}

	if pkm.ServerWALEnd > 0 {
		s.UpdateXLogPos(pkm.ServerWALEnd)
		logger.Debug("updated xlog position from keepalive", "serverWALEnd", pkm.ServerWALEnd.String())
	}

	if pkm.ReplyRequested {
		if err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos())); err != nil {
			logger.Error("standby status update", "error", err)
			return err
		}
		logger.Debug("standby status update sent on keepalive request")
	}

	return nil
}

// handleXLogData parses a WAL data message, decodes the logical replication
// event, and dispatches it through the message buffer.
func (s *stream) handleXLogData(data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) {
	xld, err := ParseXLogData(data)
	if err != nil {
		logger.Error("parse xLog data", "error", err)
		return
	}

	logger.Debug("wal received",
		"walData", string(xld.WALData),
		"walDataByte", slice.ConvertToInt(xld.WALData),
		"walStart", xld.WALStart,
		"walEnd", xld.ServerWALEnd,
		"serverTime", xld.ServerTime,
	)

	s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

	decodedMsg, err := message.New(xld.WALData, xld.ServerTime, s.relation)
	if err != nil || decodedMsg == nil {
		logger.Debug("wal data message parsing error", "error", err)
		return
	}

	s.dispatchMessage(decodedMsg, xld, buf, streamBuf)
}

// dispatchMessage routes a decoded logical replication event to the correct
// buffer action.
//
// For regular (non-streaming) transactions the messageBuffer provides a
// one-message look-ahead so the last DML's WAL position can be rewritten to
// the transaction-end LSN at COMMIT.
//
// For streaming transactions (proto v2) messages are accumulated in the
// streamTxBuffer across STREAM START / STREAM STOP chunks. They are only
// emitted to the consumer on STREAM COMMIT and discarded on STREAM ABORT.
// This prevents uncommitted data from being delivered.
func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer, streamBuf *streamTxBuffer) {
	switch msg := decodedMsg.(type) {
	case *format.Begin:
		buf.discard()

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)

	case *format.StreamStart:
		// Beginning of a streaming chunk – DML events that follow belong
		// to an in-progress transaction and must be buffered per-XID.
		streamBuf.startTx(msg.Xid)

	case *format.StreamStop:
		// End of a streaming chunk. Nothing is emitted to the consumer.
		streamBuf.stopTx()

	case *format.StreamCommit:
		// Final commit of a streamed transaction – emit all messages for this XID.
		streamBuf.flushTx(msg.Xid, buf.outCh, msg.TransactionEndLSN)

	case *format.StreamAbort:
		// Streamed transaction rolled back – discard messages for this XID.
		streamBuf.discardTx(msg.Xid)

	default:
		// DML event (Insert, Update, Delete, Relation, …)
		m := &Message{
			message:  decodedMsg,
			walStart: int64(xld.WALStart),
		}
		if streamBuf.streaming {
			streamBuf.append(m)
		} else {
			buf.buffer(m)
		}
	}
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")

	for {
		msg, ok := <-s.messageCH
		if !ok {
			break
		}

		ackFunc := func() error {
			pos := pq.LSN(msg.walStart)
			s.UpdateXLogPos(pos)
			logger.Debug("send stand by status update", "xLogPos", s.LoadXLogPos().String())
			return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()))
		}

		if s.isHeartbeatMessage(msg.message) {
			if err := ackFunc(); err != nil {
				logger.Error("heartbeat auto-ack failed", "error", err)
			}
			continue
		}

		lCtx := &ListenerContext{
			Message: msg.message,
			Ack:     ackFunc,
		}

		switch lCtx.Message.(type) {
		case *format.Insert:
			s.metric.InsertOpIncrement(1)
		case *format.Delete:
			s.metric.DeleteOpIncrement(1)
		case *format.Update:
			s.metric.UpdateOpIncrement(1)
		}

		start := time.Now().UTC()
		s.listenerFunc(lCtx)
		s.metric.SetProcessLatency(time.Since(start).Nanoseconds())
	}
}

func (s *stream) isHeartbeatMessage(msg any) bool {
	if !s.config.IsHeartbeatEnabled() {
		return false
	}

	hbSchema := s.config.Heartbeat.Table.Schema
	hbTable := s.config.Heartbeat.Table.Name

	switch m := msg.(type) {
	case *format.Insert:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Update:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Delete:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	}

	return false
}

func (s *stream) Close(ctx context.Context) {
	s.closed.Store(true)

	<-s.sinkEnd
	if !isClosed(s.sinkEnd) {
		close(s.sinkEnd)
	}
	logger.Info("postgres message sink stopped")

	if !s.conn.IsClosed() {
		_ = s.conn.Close(ctx)
		logger.Info("postgres connection closed")
	}
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) SetSnapshotLSN(lsn pq.LSN) {
	s.snapshotLSN = lsn
}

func (s *stream) UpdateXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

// fetchSnapshotLSN queries the database to get the snapshot LSN from cdc_snapshot_job table
// Uses infinite retry with exponential backoff for resilience against transient database errors
func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pq.LSN

	err := retry.Do(
		func() error {
			// Create a separate connection for querying metadata
			// Use regular DSN (not replication DSN) for normal SQL queries
			conn, err := pq.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return errors.Wrap(err, "create connection for snapshot LSN query")
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed 
				FROM cdc_snapshot_job 
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return errors.Wrap(err, "execute snapshot LSN query")
			}

			if err = resultReader.Close(); err != nil {
				return errors.Wrap(err, "close result reader")
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pq.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(errors.Wrap(err, "parse snapshot LSN: "+lsnStr))
			}

			return nil
		},
		retry.Attempts(0),                   // 0 means infinite retries
		retry.DelayType(retry.BackOffDelay), // Exponential backoff
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch snapshot LSN")
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
}

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, walWritePosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.Unix()*1000000 + int64(t.Nanosecond())/1000 - microSecFromUnixEpochToY2K)
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
