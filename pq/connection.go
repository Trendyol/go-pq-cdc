package pq

import (
	"context"
	"errors"
	"fmt"
	"time"

	retrygo "github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Connection interface {
	Connect(ctx context.Context) error
	IsClosed() bool
	Close(ctx context.Context) error
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	Frontend() *pgproto3.Frontend
	Exec(ctx context.Context, sql string) *pgconn.MultiResultReader
}

type connection struct {
	*pgconn.PgConn
	dsn string
}

func NewConnection(ctx context.Context, dsn string) (Connection, error) {
	conn := NewConnectionTemplate(dsn)
	if err := conn.Connect(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}

func NewConnectionTemplate(dsn string) Connection {
	return &connection{
		dsn: dsn,
	}
}

func (c *connection) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.PgConn != nil && !c.IsClosed() {
		return nil
	}

	conn, err := connect(ctx, c.dsn)
	if err != nil {
		return fmt.Errorf("postgres connection: %w", err)
	}
	c.PgConn = conn
	return nil
}

func (c *connection) IsClosed() bool {
	return c.PgConn == nil || c.PgConn.IsClosed()
}

func (c *connection) Close(ctx context.Context) error {
	if c.PgConn == nil {
		return nil
	}
	return c.PgConn.Close(ctx)
}

func connect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	conn, err := retrygo.DoWithData(func() (*pgconn.PgConn, error) {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		if err = conn.Ping(ctx); err != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			closeErr := conn.Close(cleanup)
			cancel()
			if closeErr != nil {
				return nil, errors.Join(err, fmt.Errorf("close connection after failed ping: %w", closeErr))
			}
			return nil, err
		}

		return conn, nil
	}, retrygo.Attempts(5), retrygo.LastErrorOnly(true), retrygo.Delay(time.Second), retrygo.DelayType(retrygo.FixedDelay), retrygo.Context(ctx))

	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("postgres connection: %w", err)
	}

	return conn, nil
}
