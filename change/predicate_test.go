package change

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsOp(t *testing.T) {
	p := IsOp(Update, Delete)

	assert.True(t, p(&Change{Op: Update}))
	assert.True(t, p(&Change{Op: Delete}))
	assert.False(t, p(&Change{Op: Insert}))
}

func TestIsTable(t *testing.T) {
	p := IsTable("users", "sales.orders")

	assert.True(t, p(&Change{Schema: "public", Table: "users"}))
	assert.True(t, p(&Change{Schema: "sales", Table: "orders"}))
	assert.False(t, p(&Change{Schema: "sales", Table: "users"}))
}

func TestNot(t *testing.T) {
	p := Not(IsOp(Delete))

	assert.True(t, p(&Change{Op: Insert}))
	assert.False(t, p(&Change{Op: Delete}))
}

func TestWhen_RunsStepsOnlyOnMatches(t *testing.T) {
	step := When(IsOp(Delete), Exclude("email"))

	deleted := &Change{Op: Delete, Old: map[string]any{"id": 1, "email": "a@b.c"}}
	inserted := &Change{Op: Insert, New: map[string]any{"id": 1, "email": "a@b.c"}}

	assert.True(t, step(deleted))
	assert.True(t, step(inserted))
	assert.Equal(t, map[string]any{"id": 1}, deleted.Old)
	assert.Equal(t, map[string]any{"id": 1, "email": "a@b.c"}, inserted.New)
}

func TestWhen_InnerDropStillDrops(t *testing.T) {
	step := When(IsTable("audit"), OnlyOps(Insert))

	assert.False(t, step(&Change{Schema: "public", Table: "audit", Op: Update}))
	assert.True(t, step(&Change{Schema: "public", Table: "audit", Op: Insert}))
	assert.True(t, step(&Change{Schema: "public", Table: "users", Op: Update}))
}

func TestWhen_NoSteps(t *testing.T) {
	assert.True(t, When(IsOp(Insert))(&Change{Op: Insert}))
}
