package change

import "strings"

// OnlyOps keeps changes whose operation is one of ops.
func OnlyOps(ops ...Op) Step {
	allowed := make(map[Op]struct{}, len(ops))
	for _, op := range ops {
		allowed[op] = struct{}{}
	}
	return func(c *Change) bool {
		_, ok := allowed[c.Op]
		return ok
	}
}

// OnlyTables keeps changes on the given tables. A name without a schema is read as public.table.
func OnlyTables(names ...string) Step {
	allowed := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !strings.Contains(name, ".") {
			name = defaultSchema + "." + name
		}
		allowed[name] = struct{}{}
	}
	return func(c *Change) bool {
		_, ok := allowed[c.FullName()]
		return ok
	}
}

// Where keeps changes for which keep returns true.
func Where(keep func(c *Change) bool) Step {
	return keep
}
