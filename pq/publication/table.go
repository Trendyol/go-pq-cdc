package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

type Table struct {
	Name            string `json:"name" yaml:"name"`
	ReplicaIdentity string `json:"replicaIdentity" yaml:"replicaIdentity"`
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
