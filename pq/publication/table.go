package publication

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

type Table struct {
	Name            string `json:"name" yaml:"name"`
	ReplicaIdentity string `json:"replicaIdentity" yaml:"replicaIdentity"`
	Schema          string `json:"schema,omitempty" yaml:"schema,omitempty"`
}

func (tc Table) Validate() error {
	if strings.TrimSpace(tc.Name) == "" {
		return errors.New("table name cannot be empty")
	}

	if !slices.Contains(ReplicaIdentityOptions, tc.ReplicaIdentity) {
		return errors.Newf("undefined replica identity option. valid identity options are: %v", ReplicaIdentityOptions)
	}

	return nil
}

type Tables []Table

func (ts Tables) Validate() error {
	if len(ts) == 0 {
		return errors.New("at least one table must be defined")
	}

	for _, t := range ts {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (ts Tables) Diff(tss Tables) Tables {
	res := Tables{}
	tssMap := make(map[string]Table)

	for _, t := range tss {
		tssMap[t.Name+t.ReplicaIdentity] = t
	}

	for _, t := range ts {
		if v, found := tssMap[t.Name+t.ReplicaIdentity]; !found || v.ReplicaIdentity != t.ReplicaIdentity {
			res = append(res, t)
		}
	}

	return res
}

// UnmarshalJSON implements the json.Unmarshaler interface for Table.
// Custom unmarshalling logic is required to avoid infinite recursion.
// Pointer receiver is used since we have to change the data in original variable
func (t *Table) UnmarshalJSON(data []byte) error {
	type Alias Table
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Set default value for Schema if it's empty
	if aux.Schema == "" {
		aux.Schema = "public"
	}

	*t = Table(*aux.Alias)
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for Table
// Custom unmarshalling logic is required to avoid infinite recursion.
// Pointer receiver is used since we have to change the data in original variable
func (t *Table) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type Alias Table
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := unmarshal(&aux); err != nil {
		return err
	}

	// Set default value for Schema if it's empty
	if aux.Schema == "" {
		aux.Schema = "public"
	}

	*t = Table(*aux.Alias)
	return nil
}
