package change

// Include keeps only the given columns in both the old and the new row.
func Include(cols ...string) Step {
	keep := toSet(cols)
	return func(c *Change) bool {
		for _, row := range []map[string]any{c.Old, c.New} {
			for col := range row {
				if _, ok := keep[col]; !ok {
					delete(row, col)
				}
			}
		}
		return true
	}
}

// Exclude removes the given columns from both the old and the new row.
func Exclude(cols ...string) Step {
	return func(c *Change) bool {
		for _, col := range cols {
			delete(c.Old, col)
			delete(c.New, col)
		}
		return true
	}
}

// Rename renames columns in both rows, from the key of names to its value.
func Rename(names map[string]string) Step {
	return func(c *Change) bool {
		c.Old = renameColumns(c.Old, names)
		c.New = renameColumns(c.New, names)
		return true
	}
}

// renameColumns builds a new map so a rename onto an existing name cannot be undone
// by a later rename in the same pass.
func renameColumns(row map[string]any, names map[string]string) map[string]any {
	if row == nil {
		return nil
	}
	renamed := make(map[string]any, len(row))
	for col, v := range row {
		if to, ok := names[col]; ok {
			col = to
		}
		renamed[col] = v
	}
	return renamed
}

func toSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}
