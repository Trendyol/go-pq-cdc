package change

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func updateChange() *Change {
	return &Change{
		Op:  Update,
		Old: map[string]any{"id": 1, "name": "old", "password": "a"},
		New: map[string]any{"id": 1, "name": "new", "password": "b"},
	}
}

func TestInclude(t *testing.T) {
	c := updateChange()

	ok := Include("id", "name")(c)

	assert.True(t, ok)
	assert.Equal(t, map[string]any{"id": 1, "name": "old"}, c.Old)
	assert.Equal(t, map[string]any{"id": 1, "name": "new"}, c.New)
}

func TestExclude(t *testing.T) {
	c := updateChange()

	ok := Exclude("password", "missing")(c)

	assert.True(t, ok)
	assert.Equal(t, map[string]any{"id": 1, "name": "old"}, c.Old)
	assert.Equal(t, map[string]any{"id": 1, "name": "new"}, c.New)
}

func TestRename(t *testing.T) {
	c := updateChange()

	ok := Rename(map[string]string{"name": "full_name", "id": "user_id"})(c)

	assert.True(t, ok)
	assert.Equal(t, map[string]any{"user_id": 1, "full_name": "old", "password": "a"}, c.Old)
	assert.Equal(t, map[string]any{"user_id": 1, "full_name": "new", "password": "b"}, c.New)
}

func TestRename_SwapsColumns(t *testing.T) {
	c := &Change{Op: Insert, New: map[string]any{"a": 1, "b": 2}}

	Rename(map[string]string{"a": "b", "b": "a"})(c)

	assert.Equal(t, map[string]any{"a": 2, "b": 1}, c.New)
}

func TestColumnSteps_NilRows(t *testing.T) {
	steps := map[string]Step{
		"include": Include("id"),
		"exclude": Exclude("id"),
		"rename":  Rename(map[string]string{"id": "user_id"}),
	}

	for name, step := range steps {
		t.Run(name, func(t *testing.T) {
			c := &Change{Op: Insert, New: map[string]any{"id": 1}}
			assert.True(t, step(c))
			assert.Nil(t, c.Old)
		})
	}
}
