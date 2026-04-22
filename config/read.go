package config

import (
	"encoding/json"
	"os"

	"github.com/go-playground/errors"
	"gopkg.in/yaml.v2"
)

// ReadConfigYAML reads and parses a YAML configuration file into a Config.
func ReadConfigYAML(path string) (Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is caller-controlled, not user input
	if err != nil {
		return Config{}, errors.Wrap(err, "read yaml config")
	}

	c := Config{}

	err = yaml.Unmarshal(b, &c)
	if err != nil {
		return Config{}, errors.Wrap(err, "yaml config file parse")
	}

	return c, nil
}

// ReadConfigJSON reads and parses a JSON configuration file into a Config.
func ReadConfigJSON(path string) (Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is caller-controlled, not user input
	if err != nil {
		return Config{}, errors.Wrap(err, "read json config")
	}

	c := Config{}

	err = json.Unmarshal(b, &c)
	if err != nil {
		return Config{}, errors.Wrap(err, "json config file parse")
	}

	return c, nil
}
