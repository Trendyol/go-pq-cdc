package change

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestConfig_Steps(t *testing.T) {
	cfg := Config{
		Operations:   []Op{"insert", Update, Delete},
		Include:      []string{"id", "name", "email", "password"},
		Exclude:      []string{"secret"},
		Mask:         []string{"email"},
		Rename:       map[string]string{"id": "user_id"},
		InsertFields: map[string]any{"source_db": "users-db", "version": 2},
	}

	steps, err := cfg.Steps()
	require.NoError(t, err)

	c := &Change{
		Op:  Update,
		Old: map[string]any{"id": 1, "name": "old", "email": "a@b.c", "password": "x", "age": 3, "secret": "s"},
		New: map[string]any{"id": 1, "name": "new", "email": "a@b.c", "password": "x", "age": 3, "secret": "s"},
	}
	require.True(t, Run(c, steps...))

	want := func(name string) map[string]any {
		return map[string]any{"user_id": 1, "name": name, "email": MaskValue, "password": "x", "source_db": "users-db", "version": 2}
	}
	assert.Equal(t, want("old"), c.Old)
	assert.Equal(t, want("new"), c.New)
}

func TestConfig_StepsFilterOperations(t *testing.T) {
	steps, err := Config{Operations: []Op{Insert}}.Steps()
	require.NoError(t, err)

	assert.True(t, Run(&Change{Op: Insert}, steps...))
	assert.False(t, Run(&Change{Op: Delete}, steps...))
}

func TestConfig_EmptyAddsNoSteps(t *testing.T) {
	steps, err := Config{}.Steps()
	require.NoError(t, err)
	assert.Empty(t, steps)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		want []string
		name string
		cfg  Config
	}{
		{name: "valid", cfg: Config{Operations: []Op{"snapshot"}, Include: []string{"id"}, Exclude: []string{"password"}}},
		{name: "unknown operation", cfg: Config{Operations: []Op{"UPSERT"}}, want: []string{`unknown operation "UPSERT"`}},
		{name: "included and excluded", cfg: Config{Include: []string{"id"}, Exclude: []string{"id"}}, want: []string{`"id" is both included and excluded`}},
		{name: "empty rename", cfg: Config{Rename: map[string]string{"id": ""}}, want: []string{`"id" has an empty new name`}},
		{
			name: "every problem at once",
			cfg:  Config{Operations: []Op{"UPSERT"}, Rename: map[string]string{"id": ""}},
			want: []string{"UPSERT", "empty new name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if len(tt.want) == 0 {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tt.want {
				assert.Contains(t, err.Error(), want)
			}

			_, err = tt.cfg.Steps()
			assert.Error(t, err)
		})
	}
}

func TestConfig_FromYAML(t *testing.T) {
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(`
operations: [INSERT, UPDATE]
exclude: [password]
mask: [email]
rename: {id: user_id}
insertFields: {source_db: users-db}
`), &cfg))

	assert.Equal(t, Config{
		Operations:   []Op{Insert, Update},
		Exclude:      []string{"password"},
		Mask:         []string{"email"},
		Rename:       map[string]string{"id": "user_id"},
		InsertFields: map[string]any{"source_db": "users-db"},
	}, cfg)
}

func TestQualifiedName(t *testing.T) {
	assert.Equal(t, "public.users", QualifiedName("users"))
	assert.Equal(t, "sales.orders", QualifiedName("sales.orders"))
}
