package change

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Config describes steps in a config file instead of code. Steps builds them in a fixed order:
// operations, include, exclude, mask, rename, insertFields. Empty fields add no step.
//
//	operations: [INSERT, UPDATE, DELETE]
//	exclude: [password]
//	mask: [email]
//	rename: {id: user_id}
//	insertFields: {source_db: users-db}
type Config struct {
	Rename       map[string]string `json:"rename" yaml:"rename"`
	InsertFields map[string]any    `json:"insertFields" yaml:"insertFields"`
	Operations   []Op              `json:"operations" yaml:"operations"`
	Include      []string          `json:"include" yaml:"include"`
	Exclude      []string          `json:"exclude" yaml:"exclude"`
	Mask         []string          `json:"mask" yaml:"mask"`
}

var knownOps = map[Op]struct{}{Insert: {}, Update: {}, Delete: {}, Snapshot: {}}

// Validate reports every problem in the config at once.
func (c Config) Validate() error {
	var errs []error
	for _, op := range c.Operations {
		if _, ok := knownOps[normalizeOp(op)]; !ok {
			errs = append(errs, fmt.Errorf("operations: unknown operation %q", op))
		}
	}
	excluded := toSet(c.Exclude)
	for _, col := range c.Include {
		if _, ok := excluded[col]; ok {
			errs = append(errs, fmt.Errorf("column %q is both included and excluded", col))
		}
	}
	for from, to := range c.Rename {
		if to == "" {
			errs = append(errs, fmt.Errorf("rename: column %q has an empty new name", from))
		}
	}
	return errors.Join(errs...)
}

// Steps validates the config and returns its steps.
func (c Config) Steps() ([]Step, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	var steps []Step
	if len(c.Operations) > 0 {
		ops := make([]Op, len(c.Operations))
		for i, op := range c.Operations {
			ops[i] = normalizeOp(op)
		}
		steps = append(steps, OnlyOps(ops...))
	}
	if len(c.Include) > 0 {
		steps = append(steps, Include(c.Include...))
	}
	if len(c.Exclude) > 0 {
		steps = append(steps, Exclude(c.Exclude...))
	}
	if len(c.Mask) > 0 {
		steps = append(steps, Mask(c.Mask...))
	}
	if len(c.Rename) > 0 {
		steps = append(steps, Rename(c.Rename))
	}
	for _, col := range slices.Sorted(maps.Keys(c.InsertFields)) {
		steps = append(steps, InsertField(col, c.InsertFields[col]))
	}
	return steps, nil
}

// QualifiedName returns a table name as schema.table, reading a name without a schema as public.table.
func QualifiedName(name string) string {
	if strings.Contains(name, ".") {
		return name
	}
	return defaultSchema + "." + name
}

func normalizeOp(op Op) Op {
	return Op(strings.ToUpper(string(op)))
}
