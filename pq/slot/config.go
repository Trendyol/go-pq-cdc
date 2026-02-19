package slot

import (
	"errors"
	"strings"
	"time"
)

type Config struct {
	Name                        string        `json:"name" yaml:"name"`
	SlotActivityCheckerInterval time.Duration `json:"slotActivityCheckerInterval" yaml:"slotActivityCheckerInterval"`
	// ProtoVersion selects the pgoutput logical replication protocol version.
	//   1 – compatible with PostgreSQL 10+; no streaming transaction support.
	//   2 – requires PostgreSQL 14+; supports streaming large in-progress transactions (default).
	ProtoVersion      int  `json:"protoVersion" yaml:"protoVersion"`
	CreateIfNotExists bool `json:"createIfNotExists" yaml:"createIfNotExists"`
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

	return err
}
