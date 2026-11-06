package change

import "strings"

// Predicate reports whether a change matches. Filters keep the changes that match;
// When runs its steps only on them.
type Predicate func(c *Change) bool

// IsOp matches changes whose operation is one of ops.
func IsOp(ops ...Op) Predicate {
	allowed := make(map[Op]struct{}, len(ops))
	for _, op := range ops {
		allowed[op] = struct{}{}
	}
	return func(c *Change) bool {
		_, ok := allowed[c.Op]
		return ok
	}
}

// IsTable matches changes on the given tables. A name without a schema is read as public.table.
func IsTable(names ...string) Predicate {
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

// Not matches the changes p does not match.
func Not(p Predicate) Predicate {
	return func(c *Change) bool {
		return !p(c)
	}
}

// OnlyOps keeps changes whose operation is one of ops.
func OnlyOps(ops ...Op) Step {
	return Step(IsOp(ops...))
}

// OnlyTables keeps changes on the given tables. A name without a schema is read as public.table.
func OnlyTables(names ...string) Step {
	return Step(IsTable(names...))
}

// Where keeps changes for which keep returns true.
func Where(keep Predicate) Step {
	return Step(keep)
}

// When runs steps only on changes that match p; other changes pass through untouched.
// A step that drops a matching change still drops it.
func When(p Predicate, steps ...Step) Step {
	return func(c *Change) bool {
		if !p(c) {
			return true
		}
		return Run(c, steps...)
	}
}
