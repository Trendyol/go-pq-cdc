package publication

import (
	"context"
	goerrors "errors"
	"fmt"

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
	conn pq.Connection
	cfg  Config
}

func New(cfg Config, conn pq.Connection) *Publication {
	return &Publication{cfg: cfg, conn: conn}
}

func (c *Publication) Create(ctx context.Context) (*Config, error) {
	info, err := c.Info(ctx)
	if err != nil {
		if !goerrors.Is(err, ErrorPublicationIsNotExists) || !c.cfg.CreateIfNotExists {
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
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "42703" {
			return nil, ErrorPublicationIsNotExists
		}
		return nil, errors.Wrap(err, "publication info result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
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
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "INSERT")
			}
		case "pubupdate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "UPDATE")
			}
		case "pubdelete":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "DELETE")
			}
		case "pubtruncate":
			if v.(bool) {
				publicationConfig.Operations = append(publicationConfig.Operations, "TRUNCATE")
			}
		case "included_tables":
			for _, val := range v.([]any) {
				publicationConfig.Tables = append(publicationConfig.Tables, Table{Name: val.(string)})
			}
		}
	}

	return &publicationConfig, nil
}

func (c *Publication) Tables(ctx context.Context) ([]string, error) {
	query := fmt.Sprintf(`SELECT pt.schemaname, pt.tablename
		FROM pg_publication_tables pt
		WHERE pt.pubname = '%s'

		UNION

		SELECT t.schemaname,
			   t.tablename
		FROM pg_tables t,
			 pg_publication p
		WHERE p.pubname = '%s'
		  AND p.puballtables = TRUE
		  AND t.schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', '_timescaledb_internal')
		  AND t.schemaname NOT LIKE 'pg_temp_%%';`, c.cfg.Name, c.cfg.Name)
	resultReader := c.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "publication tables result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, nil
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "publication tables result reader close")
	}

	tables, err := decodePublicationTablesResult(results)
	if err != nil {
		return nil, errors.Wrap(err, "publication tables result decode")
	}

	return tables, nil
}

func decodePublicationTablesResult(results []*pgconn.Result) ([]string, error) {
	res := make([]string, 0)

	for _, result := range results {
		for i := range len(result.Rows) {
			var schemaname, tablename string
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "schemaname":
					schemaname = v.(string)
				case "tablename":
					tablename = v.(string)
				}
			}

			res = append(res, fmt.Sprintf("%s.%s", schemaname, tablename))
		}
	}

	return res, nil
}

func decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(dataType); ok {
		return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}
