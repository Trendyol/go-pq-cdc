package dcpg

import (
	"context"
	"fmt"
	"github.com/3n0ugh/dcpg/config"
	"github.com/3n0ugh/dcpg/message"
	"github.com/3n0ugh/dcpg/message/format"
	"github.com/3n0ugh/dcpg/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"log/slog"
	"os"
	"time"
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

type Connector interface {
	Start(ctx context.Context) (<-chan Context, error)
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	conn             pq.Connection
	cfg              *config.Config
	metricCollectors []prometheus.Collector

	systemID pq.IdentifySystemResult
}

func NewConnector(ctx context.Context, cfg config.Config) (Connector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}

	conn, err := pq.NewConnection(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Publication.DropIfExists {
		if err = pq.DropPublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
	}

	if cfg.Publication.Create {
		if err = pq.CreatePublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
		slog.Info("publication created", "name", cfg.Publication.Name)
	}

	system, err := pq.IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}
	slog.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.XLogPos, "database:", system.Database)

	if cfg.Slot.Create {
		err = pq.CreateReplicationSlot(context.Background(), conn, cfg.Slot.Name)
		if err != nil {
			return nil, err
		}
		slog.Info("slot created", "name", cfg.Slot.Name)
	}

	return &connector{
		conn:     conn,
		cfg:      &cfg,
		systemID: system,
	}, nil
}

func (c *connector) Start(ctx context.Context) (<-chan Context, error) {
	replication := pq.NewReplication(c.conn)
	if err := replication.Start(c.cfg.Publication.Name, c.cfg.Slot.Name); err != nil {
		return nil, err
	}
	if err := replication.Test(ctx); err != nil {
		return nil, err
	}
	slog.Info("replication started", "slot", c.cfg.Slot.Name)

	relation := map[uint32]*format.Relation{}

	ch := make(chan Context, c.cfg.ChannelBuffer)
	lastXLogPos := pq.LSN(10)

	go func() {
		defer func() {
			if err := c.conn.Close(ctx); err != nil {
				slog.Error("postgres connection close", "error", err.Error())
				os.Exit(1)
			}
		}()

		for {
			msgCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*10))
			rawMsg, err := c.conn.ReceiveMessage(msgCtx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					err = pq.SendStandbyStatusUpdate(ctx, c.conn, uint64(lastXLogPos))
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
			case PrimaryKeepaliveMessageByteID:
				continue
			case XLogDataByteID:
				var xld pq.XLogData
				xld, err = pq.ParseXLogData(msg.Data[1:])
				if err != nil {
					slog.Error("parse xLog data", "error", err)
					continue
				}

				c.systemID.XLogPos = max(xld.WALStart, c.systemID.XLogPos)

				connectorCtx := Context{
					Ack: func() error {
						lastXLogPos = xld.ServerWALEnd
						return pq.SendStandbyStatusUpdate(ctx, c.conn, uint64(c.systemID.XLogPos))
					},
				}

				connectorCtx.Message, err = message.New(xld.WALData, relation)
				if err != nil || connectorCtx.Message == nil {
					// slog.Error("wal data message parsing", "error", err) // TODO: comment out after implementations
					continue
				}

				ch <- connectorCtx
			}
		}
		close(ch)
	}()

	return ch, nil
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.metricCollectors = append(c.metricCollectors, metricCollectors...)
}
