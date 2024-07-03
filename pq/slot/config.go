package slot

import (
	"errors"
	"strings"
	"time"
)

type Config struct {
	Name                        string        `json:"name" yaml:"name"`
	SlotActivityCheckerInterval time.Duration `json:"slotActivityCheckerInterval" yaml:"slotActivityCheckerInterval"`
	CreateIfNotExists           bool          `json:"createIfNotExists" yaml:"createIfNotExists"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	if c.SlotActivityCheckerInterval < 1000 {
		err = errors.Join(err, errors.New("slot activity checker interval cannot be lower than 1000 ms"))
	}

	return err
}
