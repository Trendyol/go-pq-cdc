package publication

import (
	"slices"
	"strings"

	"github.com/go-playground/errors"
)

// SnapshotPartitionStrategy defines how a table should be partitioned during snapshot.
// If empty, the strategy is auto-detected based on primary key type.
type SnapshotPartitionStrategy string

const (
	// SnapshotPartitionStrategyAuto lets the system decide based on PK type (default)
	SnapshotPartitionStrategyAuto SnapshotPartitionStrategy = ""
	// SnapshotPartitionStrategyIntegerRange uses MIN/MAX range for integer PKs
	SnapshotPartitionStrategyIntegerRange SnapshotPartitionStrategy = "integer_range"
	// SnapshotPartitionStrategyCTIDBlock uses PostgreSQL physical block locations
	SnapshotPartitionStrategyCTIDBlock SnapshotPartitionStrategy = "ctid_block"
	// SnapshotPartitionStrategyOffset uses LIMIT/OFFSET (slow, fallback)
	SnapshotPartitionStrategyOffset SnapshotPartitionStrategy = "offset"
)

// ValidSnapshotPartitionStrategies contains all valid partition strategy options
var ValidSnapshotPartitionStrategies = []SnapshotPartitionStrategy{
	SnapshotPartitionStrategyAuto,
	SnapshotPartitionStrategyIntegerRange,
	SnapshotPartitionStrategyCTIDBlock,
	SnapshotPartitionStrategyOffset,
}

type Table struct {
	Name            string `json:"name" yaml:"name"`
	ReplicaIdentity string `json:"replicaIdentity" yaml:"replicaIdentity"`
	Schema          string `json:"schema,omitempty" yaml:"schema,omitempty"`
	// SnapshotPartitionStrategy allows overriding the auto-detected partition strategy.
	// Useful when integer PKs are hash-based (not sequential) and range partitioning performs poorly.
	// Options: "" (auto), "integer_range", "ctid_block", "offset"
	SnapshotPartitionStrategy SnapshotPartitionStrategy `json:"snapshotPartitionStrategy,omitempty" yaml:"snapshotPartitionStrategy,omitempty"`
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
