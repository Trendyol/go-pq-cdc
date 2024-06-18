package publication

import (
	"context"
	goerrors "errors"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"log/slog"
)

var (
	ErrorPublicationIsNotExists = errors.New("publication is not exists")
)

type Publication struct {
	cfg  Config
	conn pq.Connection
}

func New(cfg Config, conn pq.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (c *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorPublicationIsNotExists) {
			return nil, errors.Wrap(err, "publication info")
		}
	} else {
		slog.Warn("publication already exists")
		return info, nil
	}

	resultReader := c.conn.Exec(ctx, c.cfg.createQuery())
	_, err = resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication create result")
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication create result reader close")
	}

	slog.Info("publication created", "name", c.cfg.Name)

	return &c.cfg, nil
}

func (c *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := c.conn.Exec(ctx, c.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 {
		return nil, ErrorPublicationIsNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication info result reader close")
	}

	return nil, nil
}
