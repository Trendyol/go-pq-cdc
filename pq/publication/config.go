package publication

import (
	"errors"
	"fmt"
	"github.com/lib/pq"
	"slices"
	"strings"
)

var OperationOptions = []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}

type Config struct {
	Name       string        `json:"name" yaml:"name"`
	Operations []string      `json:"operations" yaml:"operations"`
	Tables     []TableConfig `json:"tables" yaml:"tables"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}

	if len(c.Tables) == 0 {
		err = errors.Join(err, errors.New("publication table scope should be defined"))
	}

	for i, t := range c.Tables {
		if err = t.Validate(); err != nil {
			err = errors.Join(err, errors.New(fmt.Sprintf("publication table [%d]: %s\n", i, err.Error())))
		}
	}

	if len(c.Operations) == 0 {
		err = errors.Join(err, errors.New("publication must have at least one operations: "+strings.Join(OperationOptions, ", ")))
	}

	for _, op := range c.Operations {
		if !slices.Contains(OperationOptions, op) {
			err = errors.Join(err, errors.New("publication operations: "+err.Error()))
		}
	}

	return err
}

func (c Config) createQuery() string {
	sqlStatement := fmt.Sprintf(`CREATE PUBLICATION %s`, pq.QuoteIdentifier(c.Name))

	quotedTables := make([]string, len(c.Tables))
	for i, table := range c.Tables {
		quotedTables[i] = pq.QuoteIdentifier(table.Name)
	}
	sqlStatement += " FOR TABLE " + strings.Join(quotedTables, ", ")

	sqlStatement += fmt.Sprintf(" WITH (publish = '%s')", strings.Join(c.Operations, ", "))

	return sqlStatement
}

func (c Config) infoQuery() string {
	q := fmt.Sprintf(`SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, COALESCE(array_agg(c.relname) FILTER (WHERE c.relname IS NOT NULL), ARRAY[]::text[]) AS included_tables FROM pg_publication p LEFT JOIN pg_publication_rel pr ON p.oid = pr.prpubid LEFT JOIN pg_class c ON pr.prrelid = c.oid WHERE p.pubname = '%s' GROUP BY p.pubname, p.pubowner, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate;`, c.Name)
	return q
}

var ReplicaIdentityOptions = []string{"DEFAULT", "NOTHING", "FULL"}

type TableConfig struct {
	Name            string `json:"name" yaml:"name"`
	ReplicaIdentity string `json:"replicaIdentity" yaml:"replicaIdentity"`
}

func (tc TableConfig) Validate() error {
	if strings.TrimSpace(tc.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, tc.ReplicaIdentity) {
		return errors.New("undefined replica identity option")
	}

	return nil
}
