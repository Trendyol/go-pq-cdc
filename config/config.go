package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

type Config struct {
	Logger           LoggerConfig       `json:"logger" yaml:"logger"`
	Host             string             `json:"host" yaml:"host"`
	Username         string             `json:"username" yaml:"username"`
	Password         string             `json:"password" yaml:"password"`
	Database         string             `json:"database" yaml:"database"`
	Publication      publication.Config `json:"publication" yaml:"publication"`
	Slot             slot.Config        `json:"slot" yaml:"slot"`
	Port             int                `json:"port" yaml:"port"`
	Metric           MetricConfig       `json:"metric" yaml:"metric"`
	DebugMode        bool               `json:"debugMode" yaml:"debugMode"`
	ExtensionSupport ExtensionSupport   `json:"extensionSupport" yaml:"extensionSupport"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	Logger   logger.Logger `json:"-" yaml:"-"`         // custom logger
	LogLevel slog.Level    `json:"level" yaml:"level"` // if custom logger is nil, set the slog log level
}

type ExtensionSupport struct {
	EnableTimeScaleDB bool `json:"enableTimeScaleDB" yaml:"EnableTimeScaleDB"`
}

func (c *Config) DSN() string {
	// URL-encode username and password to handle special characters
	encodedUsername := url.QueryEscape(c.Username)
	encodedPassword := url.QueryEscape(c.Password)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", encodedUsername, encodedPassword, c.Host, c.Port, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	// URL-encode username and password to handle special characters
	encodedUsername := url.QueryEscape(c.Username)
	encodedPassword := url.QueryEscape(c.Password)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", encodedUsername, encodedPassword, c.Host, c.Port, c.Database)
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 5432
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}

	if c.Slot.SlotActivityCheckerInterval == 0 {
		c.Slot.SlotActivityCheckerInterval = 1000
	}

	if c.Logger.Logger == nil {
		c.Logger.Logger = logger.NewSlog(c.Logger.LogLevel)
	}

	// Set default schema names for tables
	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" {
			c.Publication.Tables[tableID].Schema = "public"
		}
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

	return err
}

func (c *Config) Print() {
	cfg := *c
	cfg.Password = "*******"
	b, _ := json.Marshal(cfg)
	fmt.Println("used config: " + string(b))
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
