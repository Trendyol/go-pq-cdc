package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

var OperationOptions = Operations{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}

var (
	OperationInsert   Operation = "INSERT"
	OperationUpdate   Operation = "UPDATE"
	OperationDelete   Operation = "DELETE"
	OperationTruncate Operation = "TRUNCATE"
)

type Operation string

func (op Operation) Validate() error {
	if !slices.Contains(OperationOptions, op) {
		return errors.Newf("undefined operation. valid operations are: %v", OperationOptions)
	}

	return nil
}

type Operations []Operation

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

func (ops Operations) String() string {
	res := make([]string, len(ops))

	for i, op := range ops {
		res[i] = string(op)
	}

	return strings.Join(res, ", ")
}
