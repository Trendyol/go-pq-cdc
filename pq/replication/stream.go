package replication

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/internal/slice"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"log/slog"
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
	GetSystemInfo() pq.IdentifySystemResult
	GetMetric() metric.Metric
}

type stream struct {
	conn   pq.Connection
	metric metric.Metric
	system pq.IdentifySystemResult
	config config.Config

	relation     map[uint32]*format.Relation
	listenerFunc ListenerFunc
	lastXLogPos  pq.LSN
	closed       bool
}

func NewStream(conn pq.Connection, cfg config.Config, m metric.Metric, system pq.IdentifySystemResult, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         conn,
		metric:       m,
		system:       system,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		listenerFunc: listenerFunc,
		lastXLogPos:  10,
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

	slog.Info("cdc stream started")

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

	slog.Info("replication started", "slot", s.config.Slot.Name)

	return nil
}

func (s *stream) sink(ctx context.Context) {
	slog.Info("postgres message sink started")

	for {
		msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		cancel()
		if err != nil {
			if s.closed {
				slog.Info("stream stopped")
				break
			}

			if pgconn.Timeout(err) {
				err = SendStandbyStatusUpdate(ctx, s.conn, uint64(s.lastXLogPos))
				if err != nil {
					slog.Error("send stand by status update", "error", err)
					break
				}
				slog.Info("send stand by status update")
				continue
			}
			slog.Error("receive message error", "error", err)
			break
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			res, _ := errMsg.MarshalJSON()
			slog.Error("receive postgres wal error: " + string(res))
			continue
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			slog.Warn(fmt.Sprintf("received undexpected message: %T", rawMsg))
			continue
		}

		switch msg.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			continue
		case message.XLogDataByteID:
			xld, err := pq.ParseXLogData(msg.Data[1:])
			if err != nil {
				slog.Error("parse xLog data", "error", err)
				continue
			}

			slog.Debug("wal received", "walData", string(xld.WALData), "walDataByte", slice.ConvertToInt(xld.WALData), "walStart", xld.WALStart, "walEnd", xld.ServerWALEnd, "serverTime", xld.ServerTime)

			s.metric.SetCDCLatency(time.Since(xld.ServerTime).Milliseconds())

			s.system.XLogPos = max(xld.WALStart, s.system.XLogPos)

			lCtx := &ListenerContext{
				Ack: func() error {
					pos := s.system.XLogPos
					s.lastXLogPos = pos
					slog.Info("send stand by status update", "xLogPos", pos.String())
					return SendStandbyStatusUpdate(ctx, s.conn, uint64(pos))
				},
			}

			lCtx.Message, err = message.New(xld.WALData, s.relation)
			if err != nil || lCtx.Message == nil {
				slog.Debug("wal data message parsing error", "error", err)
				continue
			}

			slog.Debug("wal converted to message", "message", lCtx.Message)

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
}

func (s *stream) Close(ctx context.Context) {
	s.closed = true
	_ = s.conn.Close(ctx)
}

func (s *stream) GetSystemInfo() pq.IdentifySystemResult {
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
	return uint64(t.Unix()*1000000 + int64(t.Nanosecond())/1000 - pq.MicroSecFromUnixEpochToY2K)
}