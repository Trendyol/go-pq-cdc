package dcp

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"gitlab.trendyol.com/pq-dcp/message"
	"gitlab.trendyol.com/pq-dcp/message/format"
	"log/slog"
	"os"
	"time"
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
)

var pluginArguments = []string{
	"proto_version '3'",
	"messages 'true'",
	"streaming 'true'",
}

// OUTPUT AS CHANNEL !!!!!, MAPPER IS SHIT
// ACK return and give the option for them -- do not manage it ...

type Connector struct {
	conn *pgconn.PgConn
	cfg  Config

	systemID IdentifySystemResult
}

func NewConnector(ctx context.Context, cfg Config) (*Connector, error) {
	conn, err := pgconn.Connect(ctx, cfg.DSN())
	if err != nil {
		return nil, errors.Wrap(err, "postgres connection")
	}

	if cfg.Publication.DropIfExists {
		if err = DropPublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
	}

	if cfg.Publication.Create {
		if err = CreatePublication(ctx, conn, cfg.Publication.Name); err != nil {
			return nil, err
		}
		slog.Info("publication created", "name", cfg.Publication.Name)
	}

	system, err := IdentifySystem(ctx, conn)
	if err != nil {
		return nil, err
	}
	slog.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.XLogPos, "database:", system.Database)

	if cfg.Slot.Create {
		err = CreateReplicationSlot(context.Background(), conn, cfg.Slot.Name)
		if err != nil {
			return nil, err
		}
		slog.Info("slot created", "name", cfg.Slot.Name)
	}

	return &Connector{
		conn:     conn,
		cfg:      cfg,
		systemID: system,
	}, nil
}

func (c *Connector) Start(ctx context.Context) (<-chan Context, error) {
	replication := NewReplication(c.conn)
	if err := replication.Start(c.cfg.Publication.Name, c.cfg.Slot.Name); err != nil {
		return nil, err
	}
	if err := replication.Test(ctx); err != nil {
		return nil, err
	}
	slog.Info("replication started", "slot", c.cfg.Slot.Name)

	relation := map[uint32]*format.Relation{}

	ch := make(chan Context, 128)

	go func() {
		defer func() {
			if err := c.conn.Close(ctx); err != nil {
				slog.Error("postgres connection close", "error", err.Error())
				os.Exit(1)
			}
		}()

		for {
			rawMsg, err := c.conn.ReceiveMessage(ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					slog.Warn("receive message got timeout but continue")
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
				var xld XLogData
				xld, err = ParseXLogData(msg.Data[1:])
				if err != nil {
					slog.Error("parse xLog data", "error", err)
					continue
				}

				c.systemID.XLogPos = max(xld.WALStart, c.systemID.XLogPos)

				connectorCtx := Context{
					Ack: func() error {
						return SendStandbyStatusUpdate(ctx, c.conn, uint64(c.systemID.XLogPos))
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

type XLogData struct {
	WALStart     LSN
	ServerWALEnd LSN
	ServerTime   time.Time
	WALData      []byte
}

func ParseXLogData(buf []byte) (XLogData, error) {
	var xld XLogData
	if len(buf) < 24 {
		return xld, fmt.Errorf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	xld.WALStart = LSN(binary.BigEndian.Uint64(buf))
	xld.ServerWALEnd = LSN(binary.BigEndian.Uint64(buf[8:]))
	xld.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:])))
	xld.WALData = buf[24:]

	return xld, nil
}

const microSecFromUnixEpochToY2K = 946684800 * 1000000

func pgTimeToTime(microSecSinceY2K int64) time.Time {
	return time.Unix(0, microSecFromUnixEpochToY2K+microSecSinceY2K*1000)
}
