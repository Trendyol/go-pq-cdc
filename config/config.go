package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"log/slog"
	"strings"
)

type Config struct {
	Host        string             `json:"host" yaml:"host"`
	Username    string             `json:"username" yaml:"username"`
	Password    string             `json:"password" yaml:"password"`
	Database    string             `json:"database" yaml:"database"`
	DebugMode   bool               `json:"debugMode" yaml:"debugMode"`
	Publication publication.Config `json:"publication" yaml:"publication"`
	Slot        slot.Config        `json:"slot" yaml:"slot"`
	Metric      MetricConfig       `json:"metric" yaml:"metric"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

func (c Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?replication=database", c.Username, c.Password, c.Host, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", c.Username, c.Password, c.Host, c.Database)
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

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	b, _ := json.Marshal(cfg)
	slog.Info("used config: " + string(b))
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
