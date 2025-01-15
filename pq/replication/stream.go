package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/vskurikhin/go-pq-cdc/config"
	"github.com/vskurikhin/go-pq-cdc/internal/metric"
	"github.com/vskurikhin/go-pq-cdc/internal/slice"
	"github.com/vskurikhin/go-pq-cdc/logger"
	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/vskurikhin/go-pq-cdc/pq/message"
	"github.com/vskurikhin/go-pq-cdc/pq/message/format"
)

var (
	ErrorSlotInUse = errors.New("replication slot in use")
)

const (
	StandbyStatusUpdateByteID = 'r'
)

type ListenerContext struct {
	Message any
	Ack     func() error
}

type ListenerFunc func(ctx *ListenerContext)
type SendLSNHookFunc func(xLogPos pq.LSN)
type SinkHookFunc func(xLogData *XLogData)

type Listeners interface {
	ListenerFunc() ListenerFunc
	SendLSNHookFunc() SendLSNHookFunc
	SinkHookFunc() SinkHookFunc
}

type Message struct {
	message  any
	walStart int64
}

type Streamer interface {
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
}

type stream struct {
	conn            pq.Connection
	metric          metric.Metric
	system          *pq.IdentifySystemResult
	relation        map[uint32]*format.Relation
	messageCH       chan *Message
	listenerFunc    ListenerFunc
	sendLSNHookFunc SendLSNHookFunc
	sinkHookFunc    SinkHookFunc
	sinkEnd         chan struct{}
	mu              *sync.RWMutex
	config          config.Config
	lastXLogPos     pq.LSN
	closed          atomic.Bool
}

func NewStream(conn pq.Connection, cfg config.Config, m metric.Metric, system *pq.IdentifySystemResult, listeners Listeners) Streamer {
	return &stream{
		conn:            conn,
		metric:          m,
		system:          system,
		config:          cfg,
		relation:        make(map[uint32]*format.Relation),
		messageCH:       make(chan *Message, 1000),
		listenerFunc:    listeners.ListenerFunc(),
		sendLSNHookFunc: listeners.SendLSNHookFunc(),
		sinkHookFunc:    listeners.SinkHookFunc(),
		lastXLogPos:     10,
		sinkEnd:         make(chan struct{}, 1),
		mu:              &sync.RWMutex{},
	}
}

func (s *stream) Open(ctx context.Context) error {
	if err := s.setup(ctx); err != nil {
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

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	logger.Info("replication started", "slot", s.config.Slot.Name)

	return nil
}

//nolint:funlen
func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	for {
		msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*500))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				break
			}

			if pgconn.Timeout(err) {
				xLogPos := s.LoadXLogPos()
				err = SendStandbyStatusUpdate(ctx, s.conn, uint64(xLogPos))
				if err != nil {
					logger.Error("send stand by status update", "error", err)
					break
				}
				logger.Debug("send stand by status update")
				s.sendLSNHookFunc(xLogPos)
				continue
			}
			logger.Error("receive message error", "error", err)
			panic(err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			res, _ := errMsg.MarshalJSON()
			logger.Error("receive postgres wal error: " + string(res))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			logger.Warn(fmt.Sprintf("received undexpected message: %T", rawMsg))
			continue
		}

		var xld XLogData

		switch msg.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			continue
		case message.XLogDataByteID:
			xld, err = ParseXLogData(msg.Data[1:])
			if err != nil {
				logger.Error("parse xLog data", "error", err)
				continue
			}

			logger.Debug("wal received", "walData", string(xld.WALData), "walDataByte", slice.ConvertToInt(xld.WALData), "walStart", xld.WALStart, "walEnd", xld.ServerWALEnd, "serverTime", xld.ServerTime)
			s.sinkHookFunc(&xld)
			s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

			var decodedMsg any
			decodedMsg, err = message.New(xld.WALData, xld.ServerTime, s.relation)
			if err != nil || decodedMsg == nil {
				logger.Debug("wal data message parsing error", "error", err)
				continue
			}

			s.messageCH <- &Message{
				message:  decodedMsg,
				walStart: int64(xld.WALStart),
			}
		}
	}
	s.sinkEnd <- struct{}{}
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")

	for {
		msg, ok := <-s.messageCH
		if !ok {
			break
		}

		lCtx := &ListenerContext{
			Message: msg.message,
			Ack: func() error {
				pos := pq.LSN(msg.walStart)
				s.system.UpdateXLogPos(pos)
				logger.Debug("send stand by status update", "xLogPos", pos.String())
				xLogPos := s.system.LoadXLogPos()
				err := SendStandbyStatusUpdate(ctx, s.conn, uint64(s.system.LoadXLogPos()))
				s.sendLSNHookFunc(xLogPos)
				return err
			},
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

func (s *stream) Close(ctx context.Context) {
	s.closed.Store(true)

	<-s.sinkEnd
	close(s.sinkEnd)
	logger.Info("postgres message sink stopped")

	close(s.messageCH)
	logger.Info("postgres message process stopped")

	_ = s.conn.Close(ctx)
	logger.Info("postgres connection closed")
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
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
