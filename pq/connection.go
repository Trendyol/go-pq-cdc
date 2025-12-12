package pq

import (
	"context"

	"github.com/Trendyol/go-pq-cdc/internal/retry"
	"github.com/go-playground/errors"
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
	if c.PgConn != nil && !c.IsClosed() {
		return nil
	}

	conn, err := connect(ctx, c.dsn)
	if err != nil {
		return errors.Wrap(err, "postgres connection")
	}
	c.PgConn = conn
	return nil
}

func (c *connection) IsClosed() bool {
	return c.PgConn == nil || c.PgConn.IsClosed()
}

func connect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	retryConfig := retry.OnErrorConfig[*pgconn.PgConn](5, func(err error) bool { return err == nil })
	conn, err := retryConfig.Do(func() (*pgconn.PgConn, error) {
		conn, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		if err = conn.Ping(ctx); err != nil {
			return nil, err
		}

		return conn, nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "postgres connection")
	}

	return conn, nil
}
