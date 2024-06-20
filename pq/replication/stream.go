package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/internal/slice"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"sync/atomic"
	"time"
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

type Streamer interface {
	Open(ctx context.Context) error
	Close(ctx context.Context)
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
}

type stream struct {
	conn   pq.Connection
	metric metric.Metric
	system *pq.IdentifySystemResult
	config config.Config

	relation     map[uint32]*format.Relation
	messageCH    chan any
	listenerFunc ListenerFunc
	lastXLogPos  pq.LSN
	sinkEnd      chan struct{}
	closed       atomic.Bool
}

func NewStream(conn pq.Connection, cfg config.Config, m metric.Metric, system *pq.IdentifySystemResult, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         conn,
		metric:       m,
		system:       system,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan any, 1000),
		listenerFunc: listenerFunc,
		lastXLogPos:  10,
		sinkEnd:      make(chan struct{}, 1),
	}
}

func (s *stream) Open(ctx context.Context) error {
	if err := s.setup(ctx); err != nil {
		if v, ok := err.(*pgconn.PgError); ok && v.Code == "55006" {
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

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	for {
		msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*5))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				break
			}

			if pgconn.Timeout(err) {
				err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.lastXLogPos))
				if err != nil {
					logger.Error("send stand by status update", "error", err)
					break
				}
				logger.Info("send stand by status update")
				continue
			}
			logger.Error("receive message error", "error", err)
			break
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

			s.metric.SetCDCLatency(time.Since(xld.ServerTime).Milliseconds())

			s.system.UpdateXLogPos(xld.WALStart)

			var decodedMsg any
			decodedMsg, err = message.New(xld.WALData, s.relation)
			if err != nil || decodedMsg == nil {
				logger.Debug("wal data message parsing error", "error", err)
				continue
			}

			s.messageCH <- decodedMsg
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
			Message: msg,
			Ack: func() error {
				pos := s.system.LoadXLogPos()
				s.system.UpdateXLogPos(pos)
				logger.Debug("send stand by status update", "xLogPos", pos.String())
				return SendStandbyStatusUpdate(ctx, s.conn, uint64(pos))
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

		start := time.Now()
		s.listenerFunc(lCtx)
		s.metric.SetProcessLatency(time.Since(start).Milliseconds())
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

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, WALWritePosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, WALWritePosition)
	data = AppendUint64(data, WALWritePosition)
	data = AppendUint64(data, WALWritePosition)
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
