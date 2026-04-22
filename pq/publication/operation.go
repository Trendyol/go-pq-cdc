package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

// OperationOptions lists all valid publication operations.
var OperationOptions = Operations{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}

// Operation constants for publication DML event types.
var (
	OperationInsert   Operation = "INSERT"
	OperationUpdate   Operation = "UPDATE"
	OperationDelete   Operation = "DELETE"
	OperationTruncate Operation = "TRUNCATE"
)

// Operation represents a PostgreSQL publication DML operation type.
type Operation string

// Validate checks that the operation is one of the supported types.
func (op Operation) Validate() error {
	if !slices.Contains(OperationOptions, op) {
		return errors.Newf("undefined operation. valid operations are: %v", OperationOptions)
	}

	return nil
}

// Operations is a slice of Operation values.
type Operations []Operation

// Validate checks that at least one operation is defined and all are valid.
func (ops Operations) Validate() error {
	if len(ops) == 0 {
		return errors.New("at least one operation must be defined")
	}

	for _, op := range ops {
		if err := op.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// String returns a comma-separated list of operations.
func (ops Operations) String() string {
	res := make([]string, len(ops))

	for i, op := range ops {
		res[i] = string(op)
	}

	return strings.Join(res, ", ")
}

// Contains reports whether ops includes the given operation.
func (ops Operations) Contains(op Operation) bool {
	return slices.Contains(ops, op)
}
