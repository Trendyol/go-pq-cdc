package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateQueryCondition(t *testing.T) {
	t.Run("allows simple predicate", func(t *testing.T) {
		require.NoError(t, ValidateQueryCondition("status = 'active'"))
		require.NoError(t, ValidateQueryCondition("deleted_at IS NULL"))
	})

	t.Run("rejects semicolon", func(t *testing.T) {
		err := ValidateQueryCondition("1=1; DROP TABLE users")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `";"`)
	})

	t.Run("rejects line comment", func(t *testing.T) {
		err := ValidateQueryCondition("x = 1 -- trailing")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"--"`)
	})

	t.Run("rejects block comment", func(t *testing.T) {
		err := ValidateQueryCondition("x = 1 /* evil */")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"/*"`)
	})

	t.Run("rejects dollar quotes", func(t *testing.T) {
		err := ValidateQueryCondition("x = $$foo$$")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"$$"`)
	})

	t.Run("rejects DROP keyword", func(t *testing.T) {
		err := ValidateQueryCondition("false OR DROP SCHEMA public CASCADE")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DROP")
	})

	t.Run("empty is allowed", func(t *testing.T) {
		require.NoError(t, ValidateQueryCondition(""))
		require.NoError(t, ValidateQueryCondition("   "))
	})
}
