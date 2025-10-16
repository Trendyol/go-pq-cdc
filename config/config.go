package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

type Config struct {
	Logger      LoggerConfig       `json:"logger" yaml:"logger"`
	Host        string             `json:"host" yaml:"host"`
	Username    string             `json:"username" yaml:"username"`
	Password    string             `json:"password" yaml:"password"`
	Database    string             `json:"database" yaml:"database"`
	Publication publication.Config `json:"publication" yaml:"publication"`
	Slot        slot.Config        `json:"slot" yaml:"slot"`
	Snapshot    SnapshotConfig     `json:"snapshot" yaml:"snapshot"`
	Port        int                `json:"port" yaml:"port"`
	Metric      MetricConfig       `json:"metric" yaml:"metric"`
	DebugMode   bool               `json:"debugMode" yaml:"debugMode"`
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	Logger   logger.Logger `json:"-" yaml:"-"`         // custom logger
	LogLevel slog.Level    `json:"level" yaml:"level"` // if custom logger is nil, set the slog log level
}

func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", c.Username, c.Password, c.Host, c.Port, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", c.Username, c.Password, c.Host, c.Port, c.Database)
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

	// Set default snapshot config
	if c.Snapshot.Enabled {
		if c.Snapshot.Mode == "" {
			c.Snapshot.Mode = SnapshotModeNever
		}
		if c.Snapshot.Timeout == 0 {
			c.Snapshot.Timeout = 30 * time.Minute
		}
		if c.Snapshot.BatchSize == 0 {
			c.Snapshot.BatchSize = 10_000
		}
		if c.Snapshot.CheckpointInterval == 0 {
			c.Snapshot.CheckpointInterval = 10
		}
		if c.Snapshot.MaxRetries == 0 {
			c.Snapshot.MaxRetries = 3
		}
		if c.Snapshot.RetryDelay == 0 {
			c.Snapshot.RetryDelay = 5 * time.Second
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

	if cErr := c.Snapshot.Validate(); cErr != nil {
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

type SnapshotConfig struct {
	Mode               SnapshotMode  `json:"mode" yaml:"mode"`
	Timeout            time.Duration `json:"timeout" yaml:"timeout"`
	BatchSize          int           `json:"batchSize" yaml:"batchSize"`
	CheckpointInterval int           `json:"checkpointInterval" yaml:"checkpointInterval"`
	MaxRetries         int           `json:"maxRetries" yaml:"maxRetries"`
	RetryDelay         time.Duration `json:"retryDelay" yaml:"retryDelay"`
	Enabled            bool          `json:"enabled" yaml:"enabled"`
}

func (s *SnapshotConfig) Validate() error {
	if !s.Enabled {
		return nil
	}

	validModes := []SnapshotMode{SnapshotModeInitial, SnapshotModeNever}
	isValid := false
	for _, mode := range validModes {
		if s.Mode == mode {
			isValid = true
			break
		}
	}
	if !isValid {
		return errors.New("snapshot mode must be 'initial' or 'never'")
	}

	if s.BatchSize <= 0 {
		return errors.New("snapshot batch size must be greater than 0")
	}

	if s.CheckpointInterval <= 0 {
		return errors.New("snapshot checkpoint interval must be greater than 0")
	}

	return nil
}

type SnapshotMode string

const (
	SnapshotModeInitial SnapshotMode = "initial" // İlk çalışmada snapshot al
	SnapshotModeNever   SnapshotMode = "never"   // Snapshot alma
)
