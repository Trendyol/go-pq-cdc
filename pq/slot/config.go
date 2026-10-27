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
)

type Config struct {
	Name string `json:"name" yaml:"name"`
	// LogicalMessages controls logical decoding messages: auto (default)
	// requests them when the server supports them, on requires them, off never
	// requests them.
	LogicalMessages             LogicalMessagesMode `json:"logicalMessages,omitempty" yaml:"logicalMessages,omitempty"`
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
	case LogicalMessagesAuto, LogicalMessagesOn, LogicalMessagesOff:
	default:
		err = errors.Join(err, errors.New("slot logicalMessages must be one of: auto, on, off"))
	}

	return err
}
