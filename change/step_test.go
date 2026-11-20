package change

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRun_RunsStepsInOrder(t *testing.T) {
	var calls []string
	record := func(name string) Step {
		return func(*Change) bool {
			calls = append(calls, name)
			return true
		}
	}

	ok := Run(&Change{}, record("first"), record("second"))

	assert.True(t, ok)
	assert.Equal(t, []string{"first", "second"}, calls)
}

func TestRun_StopsAtFirstDrop(t *testing.T) {
	called := false
	drop := func(*Change) bool { return false }
	after := func(*Change) bool {
		called = true
		return true
	}

	ok := Run(&Change{}, drop, after)

	assert.False(t, ok)
	assert.False(t, called)
}

func TestRun_NoSteps(t *testing.T) {
	assert.True(t, Run(&Change{}))
}

func TestOnlyOps(t *testing.T) {
	step := OnlyOps(Insert, Delete)

	assert.True(t, step(&Change{Op: Insert}))
	assert.True(t, step(&Change{Op: Delete}))
	assert.False(t, step(&Change{Op: Update}))
	assert.False(t, step(&Change{Op: Snapshot}))
}

func TestOnlyTables(t *testing.T) {
	step := OnlyTables("users", "sales.orders")

	assert.True(t, step(&Change{Schema: "public", Table: "users"}))
	assert.True(t, step(&Change{Schema: "sales", Table: "orders"}))
	assert.False(t, step(&Change{Schema: "sales", Table: "users"}))
	assert.False(t, step(&Change{Schema: "public", Table: "orders"}))
}

func TestWhere(t *testing.T) {
	step := Where(func(c *Change) bool { return c.New["active"] == true })

	assert.True(t, step(&Change{New: map[string]any{"active": true}}))
	assert.False(t, step(&Change{New: map[string]any{"active": false}}))
}
