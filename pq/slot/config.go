package slot

import (
	"errors"
	"strings"
	"time"
)

// LogicalMessagesMode controls the pgoutput messages option, which carries
// logical decoding messages written by pg_logical_emit_message. The option
// exists from PostgreSQL 14 on.
type LogicalMessagesMode string

const (
	// LogicalMessagesAuto requests the option when the server supports it and
	// omits it otherwise.
	LogicalMessagesAuto LogicalMessagesMode = ""
	// LogicalMessagesOn requests the option and fails startup when the server
	// is older than PostgreSQL 14.
	LogicalMessagesOn LogicalMessagesMode = "on"
	// LogicalMessagesOff never requests the option, so no message is delivered.
	LogicalMessagesOff LogicalMessagesMode = "off"
	// logicalMessagesAutoLiteral is the spelling config files use for the default,
	// which LogicalMessagesAuto represents as the empty string.
	logicalMessagesAutoLiteral LogicalMessagesMode = "auto"

	// DefaultStreamSpillMaxMemoryBytes is the default global in-memory byte budget
	// for buffered streamed transactions when slot.spill.enabled is true (64 MiB).
	DefaultStreamSpillMaxMemoryBytes int64 = 64 * 1024 * 1024
)

// StreamSpillConfig bounds memory held by streamed in-progress transactions
// (slot.protoVersion >= 2) by spilling the largest transactions to disk.
type StreamSpillConfig struct {
	SpillDir       string `json:"spillDir,omitempty" yaml:"spillDir,omitempty"`
	MaxMemoryBytes int64  `json:"maxMemoryBytes,omitempty" yaml:"maxMemoryBytes,omitempty"`
	MaxDiskBytes   int64  `json:"maxDiskBytes,omitempty" yaml:"maxDiskBytes,omitempty"`
	Enabled        bool   `json:"enabled,omitempty" yaml:"enabled,omitempty"`
}

type Config struct {
	Name string `json:"name" yaml:"name"`
	// LogicalMessages controls logical decoding messages: auto (default)
	// requests them when the server supports them, on requires them, off never
	// requests them.
	LogicalMessages             LogicalMessagesMode `json:"logicalMessages,omitempty" yaml:"logicalMessages,omitempty"`
	Spill                       StreamSpillConfig   `json:"spill,omitempty" yaml:"spill,omitempty"`
	SlotActivityCheckerInterval time.Duration       `json:"slotActivityCheckerInterval" yaml:"slotActivityCheckerInterval"`
	// ProtoVersion selects the pgoutput logical replication protocol version.
	//   1 – compatible with PostgreSQL 10+; no streaming transaction support.
	//   2 – requires PostgreSQL 14+; supports streaming large in-progress transactions (default).
	ProtoVersion int `json:"protoVersion" yaml:"protoVersion"`

	CreateIfNotExists bool `json:"createIfNotExists" yaml:"createIfNotExists"`
	// Failover creates the slot with FAILOVER true (PostgreSQL 17+) so standbys
	// running sync_replication_slots keep a synchronized copy; an existing slot
	// is altered. Rejected with a clear error on older servers.
	Failover bool `json:"failover" yaml:"failover"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	if c.SlotActivityCheckerInterval < 1000 {
		err = errors.Join(err, errors.New("slot activity checker interval cannot be lower than 1000 ms"))
	}

	if c.ProtoVersion != 0 && c.ProtoVersion != 1 && c.ProtoVersion != 2 {
		err = errors.Join(err, errors.New("slot protoVersion must be 1 or 2"))
	}

	switch c.LogicalMessages {
	case LogicalMessagesAuto, logicalMessagesAutoLiteral, LogicalMessagesOn, LogicalMessagesOff:
	default:
		err = errors.Join(err, errors.New("slot logicalMessages must be one of: auto, on, off"))
	}

	if !c.Spill.Enabled {
		if c.Spill.MaxMemoryBytes != 0 {
			err = errors.Join(err, errors.New("slot.spill.maxMemoryBytes requires slot.spill.enabled: true"))
		}
		if c.Spill.MaxDiskBytes != 0 {
			err = errors.Join(err, errors.New("slot.spill.maxDiskBytes requires slot.spill.enabled: true"))
		}
		if strings.TrimSpace(c.Spill.SpillDir) != "" {
			err = errors.Join(err, errors.New("slot.spill.spillDir requires slot.spill.enabled: true"))
		}
	} else {
		if c.ProtoVersion == 1 {
			err = errors.Join(err, errors.New("slot.spill requires slot.protoVersion >= 2"))
		}
		if c.Spill.MaxMemoryBytes <= 0 {
			err = errors.Join(err, errors.New("slot.spill.maxMemoryBytes must be greater than 0"))
		}
		if c.Spill.MaxDiskBytes < 0 {
			err = errors.Join(err, errors.New("slot.spill.maxDiskBytes must not be negative"))
		}
		if c.Spill.MaxDiskBytes > 0 && c.Spill.MaxMemoryBytes > 0 && c.Spill.MaxDiskBytes < c.Spill.MaxMemoryBytes {
			err = errors.Join(err, errors.New("slot.spill.maxDiskBytes must be greater than or equal to slot.spill.maxMemoryBytes when non-zero"))
		}
	}

	return err
}
