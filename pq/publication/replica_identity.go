package publication

import (
	"context"
	goerrors "errors"
	"fmt"
	"strings"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
)

const (
	ReplicaIdentityFull       = "FULL"
	ReplicaIdentityDefault    = "DEFAULT"
	ReplicaIdentityNothing    = "NOTHING"
	ReplicaIdentityUsingIndex = "USING INDEX"
)

var (
	ErrorTablesNotExists   = goerrors.New("table does not exists")
	ReplicaIdentityOptions = []string{ReplicaIdentityDefault, ReplicaIdentityFull, ReplicaIdentityNothing, ReplicaIdentityUsingIndex}
	ReplicaIdentityMap     = map[string]string{
		"d": ReplicaIdentityDefault,    // primary key on old value
		"f": ReplicaIdentityFull,       // full row on old value
		"n": ReplicaIdentityNothing,    // nothing on old value
		"i": ReplicaIdentityUsingIndex, // index columns on old value
	}
)

func (c *Publication) ApplyReplicaIdentities(ctx context.Context) error {
	mismatches, _, err := c.replicaIdentityMismatches(ctx)
	if err != nil {
		return err
	}
	for _, d := range mismatches {
		if err = c.AlterTableReplicaIdentity(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (c *Publication) CheckReplicaIdentities(ctx context.Context) error {
	mismatches, actual, err := c.replicaIdentityMismatches(ctx)
	if err != nil {
		return err
	}
	if len(mismatches) == 0 {
		return nil
	}

	var details strings.Builder
	for i, mismatch := range mismatches {
		if i > 0 {
			details.WriteString("; ")
		}
		details.WriteString(fmt.Sprintf("%s.%s: configured=%s, actual=%s",
			mismatch.Schema, mismatch.Name, formatReplicaIdentity(mismatch), findActualIdentity(actual, mismatch)))
	}
	return fmt.Errorf("replica identity mismatch: %s", details.String())
}

func (c *Publication) replicaIdentityMismatches(ctx context.Context) (Tables, []Table, error) {
	configured := make(Tables, 0, len(c.cfg.Tables))
	for _, t := range c.cfg.Tables {
		if t.ReplicaIdentity != "" {
			configured = append(configured, t)
		}
	}
	if len(configured) == 0 {
		return nil, nil, nil
	}

	c.warnNothingReplicaIdentityWithUpdateDelete()

	actual, err := c.GetReplicaIdentities(ctx)
	if err != nil {
		return nil, nil, err
	}

	return identityOnlyTables(configured).Diff(actual), actual, nil
}

// identityOnlyTables keeps only fields used for replica-identity comparison so
// Diff does not treat publication Columns/Partitioned as identity mismatches.
func identityOnlyTables(tables Tables) Tables {
	out := make(Tables, len(tables))
	for i, t := range tables {
		out[i] = Table{
			Schema:               t.Schema,
			Name:                 t.Name,
			ReplicaIdentity:      t.ReplicaIdentity,
			ReplicaIdentityIndex: t.ReplicaIdentityIndex,
		}
	}
	return out
}

func findActualIdentity(tables []Table, configured Table) string {
	for _, t := range tables {
		if t.Schema == configured.Schema && t.Name == configured.Name {
			return formatReplicaIdentity(t)
		}
	}
	return "unknown"
}

func formatReplicaIdentity(t Table) string {
	identity := t.ReplicaIdentity
	if identity == "" {
		identity = ReplicaIdentityDefault
	}
	if identity == ReplicaIdentityUsingIndex && t.ReplicaIdentityIndex != "" {
		return fmt.Sprintf("%s %s", identity, t.ReplicaIdentityIndex)
	}
	return identity
}

func (c *Publication) warnNothingReplicaIdentityWithUpdateDelete() {
	hasUpdateDelete := c.cfg.Operations.Contains(OperationUpdate) || c.cfg.Operations.Contains(OperationDelete)
	if !hasUpdateDelete {
		return
	}

	for _, table := range c.cfg.Tables {
		if table.ReplicaIdentity == ReplicaIdentityNothing {
			logger.Error(
				"table uses REPLICA IDENTITY NOTHING with UPDATE/DELETE publication operations",
				"table", qualifiedTableName(table),
				"note", "NOTHING is typically suitable for insert-only scenarios",
			)
		}
	}
}

func (c *Publication) AlterTableReplicaIdentity(ctx context.Context, t Table) error {
	tableName := fmt.Sprintf("%s.%s", pq.QuoteIdentifier(t.Schema), pq.QuoteIdentifier(t.Name))
	query := fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s;", tableName, t.ReplicaIdentity)
	if t.ReplicaIdentity == ReplicaIdentityUsingIndex {
		query = fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;", tableName, pq.QuoteIdentifier(t.ReplicaIdentityIndex))
	}

	resultReader := c.conn.Exec(ctx, query)
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
		tableNames[i] = pq.QuoteLiteral(qualifiedTableName(t))
	}

	query := fmt.Sprintf(`
		SELECT
			c.relname AS table_name,
			n.nspname AS schema_name,
			c.relreplident AS replica_identity,
			idx.relname AS replica_identity_index
		FROM pg_class c
		JOIN pg_namespace n ON c.relnamespace = n.oid
		LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisreplident
		LEFT JOIN pg_class idx ON idx.oid = i.indexrelid
		WHERE concat(n.nspname, '.', c.relname) IN (%s)
	`, strings.Join(tableNames, ", "))

	logger.Debug("executing query: ", query)

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
				case "schema_name":
					t.Schema = v.(string)
				case "replica_identity":
					t.ReplicaIdentity = mapReplicaIdentity(v)
				case "replica_identity_index":
					t.ReplicaIdentityIndex = v.(string)
				}
			}
			res = append(res, t)
		}
	}

	return res, nil
}

func mapReplicaIdentity(raw any) string {
	var code string

	switch v := raw.(type) {
	case int32:
		code = string(v)
	case string:
		code = v
	default:
		return ""
	}

	if identity, ok := ReplicaIdentityMap[code]; ok {
		return identity
	}

	return code
}

func qualifiedTableName(t Table) string {
	if t.Schema == "" {
		parts := strings.SplitN(t.Name, ".", 2)
		if len(parts) == 2 {
			return parts[0] + "." + parts[1]
		}
		return "public." + t.Name
	}

	return t.Schema + "." + t.Name
}
