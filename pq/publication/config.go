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
		quotedTables[i] = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(table.Schema), pq.QuoteIdentifier(table.Name))
	}
	sqlStatement += " FOR TABLE " + strings.Join(quotedTables, ", ")

	sqlStatement += fmt.Sprintf(" WITH (publish = '%s')", c.Operations.String())

	return sqlStatement
}

func (c Config) infoQuery() string {
	q := fmt.Sprintf(`WITH publication_details AS (
    SELECT
        p.oid AS pubid,
        p.pubname,
        p.puballtables,
        p.pubinsert,
        p.pubupdate,
        p.pubdelete,
        p.pubtruncate
    FROM pg_publication p
    WHERE p.pubname = '%s'
	),
	expanded_tables AS (
		SELECT
			pubname,
			array_agg(schemaname || '.' || tablename) AS tables
		FROM pg_publication_tables
		WHERE pubname = '%s'
		GROUP BY pubname
	)
	SELECT
		pd.pubname,
		pd.puballtables,
		pd.pubinsert,
		pd.pubupdate,
		pd.pubdelete,
		pd.pubtruncate,
		COALESCE(et.tables, ARRAY[]::text[]) AS pubtables
	FROM publication_details pd
	LEFT JOIN expanded_tables et ON pd.pubname = et.pubname;`, c.Name, c.Name)
	return q
}
