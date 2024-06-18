package publication

import (
	"errors"
	"fmt"
	"github.com/lib/pq"
	"strings"
)

type Config struct {
	Name           string   `json:"name" yaml:"name"`
	AllTables      bool     `json:"allTables" yaml:"allTables"`
	Insert         bool     `json:"insert" yaml:"insert"`
	Update         bool     `json:"update" yaml:"update"`
	Delete         bool     `json:"delete" yaml:"delete"`
	Truncate       bool     `json:"truncate" yaml:"truncate"`
	IncludedTables []string `json:"includedTables" yaml:"includedTables"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}

	if !c.AllTables && len(c.IncludedTables) == 0 {
		err = errors.Join(err, errors.New("publication table scope should be defined"))
	}

	if !c.Update && !c.Delete && !c.Truncate && !c.Insert {
		err = errors.Join(err, errors.New("publication must have at least one action"))
	}

	return err
}

func (c Config) createQuery() string {
	sqlStatement := fmt.Sprintf(`CREATE PUBLICATION %s`, pq.QuoteIdentifier(c.Name))

	var operations []string
	if c.Insert {
		operations = append(operations, "INSERT")
	}
	if c.Update {
		operations = append(operations, "UPDATE")
	}
	if c.Delete {
		operations = append(operations, "DELETE")
	}
	if c.Truncate {
		operations = append(operations, "TRUNCATE")
	}

	sqlStatement += " FOR " + strings.Join(operations, ", ")

	if c.AllTables {
		sqlStatement += " FOR ALL TABLES"
	} else if len(c.IncludedTables) > 0 {
		quotedTables := make([]string, len(c.IncludedTables))
		for i, table := range c.IncludedTables {
			quotedTables[i] = pq.QuoteIdentifier(table)
		}
		sqlStatement += " TABLE " + strings.Join(quotedTables, ", ")
	}

	return sqlStatement
}

func (c Config) infoQuery() string {
	return fmt.Sprintf(`
		SELECT 
			p.pubname, 
			pg_get_userbyid(p.pubowner) AS owner, 
			p.puballtables, 
			p.pubinsert, 
			p.pubupdate, 
			p.pubdelete, 
			p.pubtruncate,
			COALESCE(array_agg(c.relname) FILTER (WHERE c.relname IS NOT NULL), ARRAY[]::text[]) AS included_tables
		FROM 
			pg_publication p
		LEFT JOIN 
			pg_publication_rel pr ON p.oid = pr.prpubid
		LEFT JOIN 
			pg_class c ON pr.prrelid = c.oid
		WHERE 
			p.pubname = %s
		GROUP BY 
			p.pubname, p.pubowner, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate;
	`, pq.QuoteIdentifier(c.Name))
}
