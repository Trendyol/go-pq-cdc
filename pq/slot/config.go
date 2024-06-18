package slot

import (
	"errors"
	"strings"
)

type Config struct {
	Name string `json:"name" yaml:"name"`
}

func (c Config) Validate() error {
	var err error
	if strings.TrimSpace(c.Name) == "" {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	return err
}
