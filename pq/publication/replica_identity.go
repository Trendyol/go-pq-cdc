package publication

import (
	"context"
	goerrors "errors"
	"fmt"
	"strings"

	"github.com/vskurikhin/go-pq-cdc/logger"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	ReplicaIdentityFull    = "FULL"
	ReplicaIdentityDefault = "DEFAULT"
)

var (
	ErrorTablesNotExists   = goerrors.New("table is not exists")
	ReplicaIdentityOptions = []string{ReplicaIdentityDefault, ReplicaIdentityFull}
	ReplicaIdentityMap     = map[string]string{
		"d": ReplicaIdentityDefault, // primary key on old value
		"f": ReplicaIdentityFull,    // full row on old value
	}
)

func (c *Publication) SetReplicaIdentities(ctx context.Context) error {
	if !c.cfg.CreateIfNotExists {
		return nil
	}

	tables, err := c.GetReplicaIdentities(ctx)
	if err != nil {
		return err
	}

	diff := c.cfg.Tables.Diff(tables)

	for _, d := range diff {
		if err = c.AlterTableReplicaIdentity(ctx, d); err != nil {
			return err
		}
	}

	return nil
}

func (c *Publication) AlterTableReplicaIdentity(ctx context.Context, t Table) error {
	resultReader := c.conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s;", t.Name, t.ReplicaIdentity))
	_, err := resultReader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "table replica identity update result")
	}

	if err = resultReader.Close(); err != nil {
		return errors.Wrap(err, "table replica identity update result reader close")
	}

	logger.Info("table replica identity updated", "name", t.Name, "replica_identity", t.ReplicaIdentity)

	return nil
}

func (c *Publication) GetReplicaIdentities(ctx context.Context) ([]Table, error) {
	tableNames := make([]string, len(c.cfg.Tables))

	for i, t := range c.cfg.Tables {
		tableNames[i] = "'" + t.Name + "'"
	}

	query := "SELECT relname AS table_name, relreplident AS replica_identity FROM pg_class WHERE relname IN (" + strings.Join(tableNames, ", ") + ")"

	resultReader := c.conn.Exec(ctx, query)
	results, err := resultReader.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "replica identities result")
	}

	if len(results) == 0 || results[0].CommandTag.String() == "SELECT 0" {
		return nil, ErrorTablesNotExists
	}

	if err = resultReader.Close(); err != nil {
		return nil, errors.Wrap(err, "replica identities result reader close")
	}

	replicaIdentities, err := decodeReplicaIdentitiesResult(results)
	if err != nil {
		return nil, errors.Wrap(err, "replica identities result decode")
	}

	return replicaIdentities, nil
}

func decodeReplicaIdentitiesResult(results []*pgconn.Result) ([]Table, error) {
	var res []Table

	for _, result := range results {
		for i := range len(result.Rows) {
			var t Table
			for j, fd := range result.FieldDescriptions {
				v, err := decodeTextColumnData(result.Rows[i][j], fd.DataTypeOID)
				if err != nil {
					return nil, err
				}

				if v == nil {
					continue
				}

				switch fd.Name {
				case "table_name":
					t.Name = v.(string)
				case "replica_identity":
					t.ReplicaIdentity = ReplicaIdentityMap[string(v.(int32))]
				}
			}
			res = append(res, t)
		}
	}

	return res, nil
}
