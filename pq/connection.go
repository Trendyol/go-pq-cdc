package pq

import (
	"context"

	"github.com/vskurikhin/go-pq-cdc/internal/retry"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Connection interface {
	Close(ctx context.Context) error
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	Frontend() *pgproto3.Frontend
	Exec(ctx context.Context, sql string) *pgconn.MultiResultReader
}

func NewConnection(ctx context.Context, dsn string) (Connection, error) {
	retryConfig := retry.OnErrorConfig[Connection](5, func(err error) bool { return err == nil })
	conn, err := retryConfig.Do(func() (Connection, error) {
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
