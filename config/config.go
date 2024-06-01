package config

import (
	"errors"
	"fmt"
	"strings"
)

type Config struct {
	Host        string            `json:"host" yaml:"host"`
	Username    string            `json:"username" yaml:"username"`
	Password    string            `json:"password" yaml:"password"`
	Database    string            `json:"database" yaml:"database"`
	DebugMode   bool              `json:"debugMode" yaml:"debugMode"`
	Publication PublicationConfig `json:"publication" yaml:"publication"`
	Slot        SlotConfig        `json:"slot" yaml:"slot"`
	Metric      MetricConfig      `json:"metric" yaml:"metric"`
}

type PublicationConfig struct {
	Name         string `json:"name" yaml:"name"`
	Create       bool   `json:"create" yaml:"create"`
	DropIfExists bool   `json:"dropIfExists" yaml:"dropIfExists"`
}

type SlotConfig struct {
	Name   string `json:"name" yaml:"name"`
	Create bool   `json:"create" yaml:"create"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?replication=database", c.Username, c.Password, c.Host, c.Database)
}

func (c *Config) SetDefault() {
	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}
}

func (c *Config) Validate() error {
	var err error
	if isEmpty(c.Host) {
		err = errors.Join(err, errors.New("host cannot be empty"))
	}

	if isEmpty(c.Username) {
		err = errors.Join(err, errors.New("username cannot be empty"))
	}

	if isEmpty(c.Password) {
		err = errors.Join(err, errors.New("password cannot be empty"))
	}

	if isEmpty(c.Database) {
		err = errors.Join(err, errors.New("database cannot be empty"))
	}

	if cErr := c.Publication.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	if cErr := c.Slot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	return nil
}

func (pc *PublicationConfig) Validate() error {
	var err error
	if isEmpty(pc.Name) {
		err = errors.Join(err, errors.New("publication name cannot be empty"))
	}

	return err
}

func (sc *SlotConfig) Validate() error {
	var err error
	if isEmpty(sc.Name) {
		err = errors.Join(err, errors.New("slot name cannot be empty"))
	}

	return err
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
