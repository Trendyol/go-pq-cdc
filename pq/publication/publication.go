package publication

import (
	"context"
	goerrors "errors"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorPublicationIsNotExists = goerrors.New("publication is not exists")
)

var typeMap = pgtype.NewMap()

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
		logger.Warn("publication already exists")
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

	logger.Info("publication created", "name", c.cfg.Name)

	return &c.cfg, nil
}

func (c *Publication) Info(ctx context.Context) (*Config, error) {
	resultReader := c.conn.Exec(ctx, c.cfg.infoQuery())
	results, err := resultReader.ReadAll()
	if err != nil {
		if v, ok := err.(*pgconn.PgError); ok && v.Code == "42703" {
			return nil, ErrorPublicationIsNotExists
		}
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 {
		return nil, ErrorPublicationIsNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication info result reader close")
	}

	publicationInfo, err := decodePublicationInfoResult(results[0])
	if err != nil {
		return nil, errors.Wrap(err, "publication info result decode")
	}

	return publicationInfo, nil
}

func decodePublicationInfoResult(result *pgconn.Result) (*Config, error) {
	var publicationConfig Config
	for i, fd := range result.FieldDescriptions {
		v, err := decodeTextColumnData(result.Rows[0][i], fd.DataTypeOID)
		if err != nil {
			return nil, err
		}

		if v == nil {
			continue
		}

		switch fd.Name {
		case "pubname":
			publicationConfig.Name = v.(string)
		case "pubinsert":
			publicationConfig.Insert = v.(bool)
		case "pubupdate":
			publicationConfig.Update = v.(bool)
		case "pubdelete":
			publicationConfig.Delete, _ = v.(bool)
		case "pubtruncate":
			publicationConfig.Truncate, _ = v.(bool)
		case "included_tables":
			for _, val := range v.([]any) {
				publicationConfig.IncludedTables = append(publicationConfig.IncludedTables, val.(string))
			}
		}
	}

	return &publicationConfig, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
