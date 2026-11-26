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

// MaskValue is what Mask writes in place of a column's value.
const MaskValue = "***"

// Mask replaces the values of the given columns with MaskValue in both rows,
// so consumers see that the column exists without seeing its value. NULL values stay NULL.
func Mask(cols ...string) Step {
	return func(c *Change) bool {
		for _, row := range []map[string]any{c.Old, c.New} {
			for _, col := range cols {
				if v, ok := row[col]; ok && v != nil {
					row[col] = MaskValue
				}
			}
		}
		return true
	}
}

// InsertField sets col to value in every row the change carries, overwriting an existing column.
func InsertField(col string, value any) Step {
	return func(c *Change) bool {
		for _, row := range []map[string]any{c.Old, c.New} {
			if row != nil {
				row[col] = value
			}
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
