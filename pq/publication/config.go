package publication

import (
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type Config struct {
	Name              string     `json:"name" yaml:"name"`
	Operations        Operations `json:"operations" yaml:"operations"`
	Tables            Tables     `json:"tables" yaml:"tables"`
	CreateIfNotExists bool       `json:"createIfNotExists" yaml:"createIfNotExists"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}

	if !c.CreateIfNotExists {
		return err
	}

	if validateErr := c.Tables.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
	}

	if validateErr := c.Operations.Validate(); validateErr != nil {
		err = errors.Join(err, validateErr)
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

	sqlStatement += fmt.Sprintf(" WITH (publish = '%s')", c.Operations.String())

	return sqlStatement
}

func (c Config) infoQuery() string {
	q := fmt.Sprintf(`SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, COALESCE(array_agg(c.relname) FILTER (WHERE c.relname IS NOT NULL), ARRAY[]::text[]) AS included_tables FROM pg_publication p LEFT JOIN pg_publication_rel pr ON p.oid = pr.prpubid LEFT JOIN pg_class c ON pr.prrelid = c.oid WHERE p.pubname = '%s' GROUP BY p.pubname, p.pubowner, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate;`, c.Name)
	return q
}
