package pq

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Trendyol/go-pq-cdc/internal/retry"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Connection interface {
	IsClosed() bool
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

type PoolConnection interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

func NewPoolConnection(ctx context.Context, dsn string) (PoolConnection, error) {
	retryConfig := retry.OnErrorConfig[PoolConnection](5, func(err error) bool { return err == nil })
	conn, err := retryConfig.Do(func() (PoolConnection, error) {
		conn, err := pgxpool.New(ctx, dsn)
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
