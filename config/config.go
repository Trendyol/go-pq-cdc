package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

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
	Snapshot         SnapshotConfig     `json:"snapshot" yaml:"snapshot"`
	Port             int                `json:"port" yaml:"port"`
	Metric           MetricConfig       `json:"metric" yaml:"metric"`
	DebugMode        bool               `json:"debugMode" yaml:"debugMode"`
	ExtensionSupport ExtensionSupport   `json:"extensionSupport" yaml:"extensionSupport"`
	Heartbeat        HeartbeatConfig    `json:"heartbeat" yaml:"heartbeat"`
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

type HeartbeatConfig struct {
	Enabled  bool          `json:"enabled" yaml:"enabled"`
	Interval time.Duration `json:"interval" yaml:"interval"`
	Query    string        `json:"query" yaml:"query"`
}

// DSN returns a normal PostgreSQL connection string for regular database operations
// (publication, metadata, snapshot chunks, etc.)
func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

// ReplicationDSN returns a replication connection string for CDC streaming
// This connection counts against max_wal_senders limit
func (c *Config) ReplicationDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) DSNWithoutSSL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", url.QueryEscape(c.Username), url.QueryEscape(c.Password), c.Host, c.Port, c.Database)
}

func (c *Config) SetDefault() {
	if c.Port == 0 {
		c.Port = 5432
	}

	if c.Metric.Port == 0 {
		c.Metric.Port = 8080
	}

	// Default heartbeat interval if enabled but not set
	if c.Heartbeat.Enabled && c.Heartbeat.Interval == 0 {
		c.Heartbeat.Interval = 5 * time.Second
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
		if c.Snapshot.ChunkSize == 0 {
			c.Snapshot.ChunkSize = 8_000
		}
		if c.Snapshot.ClaimTimeout == 0 {
			c.Snapshot.ClaimTimeout = 30 * time.Second
		}
		if c.Snapshot.HeartbeatInterval == 0 {
			c.Snapshot.HeartbeatInterval = 5 * time.Second
		}

		// Set default schema names for snapshot tables
		for tableID, table := range c.Snapshot.Tables {
			if table.Schema == "" {
				c.Snapshot.Tables[tableID].Schema = "public"
			}
		}
	}
}

// IsSnapshotOnlyMode returns true if snapshot is enabled and mode is snapshot_only
func (c *Config) IsSnapshotOnlyMode() bool {
	return c.Snapshot.Enabled && c.Snapshot.Mode == SnapshotModeSnapshotOnly
}

// GetSnapshotTables returns the tables to snapshot based on the configuration and publication info.
// For snapshot_only mode: uses snapshot.tables (independent from publication)
// For initial mode (snapshot + CDC):
//   - If snapshot.tables specified: validates it's a subset of publication tables and returns snapshot.tables
//   - If snapshot.tables not specified: returns all tables from publication
func (c *Config) GetSnapshotTables(publicationInfo *publication.Config) (publication.Tables, error) {
	// Mode 1: snapshot_only - independent from publication
	if c.IsSnapshotOnlyMode() {
		if len(c.Snapshot.Tables) == 0 {
			return nil, errors.New("snapshot.tables must be specified for snapshot_only mode")
		}
		return c.Snapshot.Tables, nil
	}

	// Mode 2: initial (snapshot + CDC)
	// If snapshot.tables specified, validate it's a subset of publication tables
	if len(c.Snapshot.Tables) > 0 {
		return c.validateSnapshotSubset(publicationInfo.Tables)
	}

	// Mode 3: initial with no snapshot.tables specified
	// Use all tables from publication (current behavior)
	return publicationInfo.Tables, nil
}

// validateSnapshotSubset ensures snapshot.tables is a subset of publication tables
// and returns the validated snapshot tables with publication metadata (like replica identity)
func (c *Config) validateSnapshotSubset(pubTables publication.Tables) (publication.Tables, error) {
	if len(pubTables) == 0 {
		return nil, errors.New("publication has no tables defined. Either specify tables in publication.tables or query an existing publication")
	}

	// Create map of publication tables for quick lookup
	pubMap := make(map[string]publication.Table)
	for _, t := range pubTables {
		key := t.Schema + "." + t.Name
		pubMap[key] = t
	}

	// Validate each snapshot table exists in publication
	validatedTables := make(publication.Tables, 0, len(c.Snapshot.Tables))
	for _, st := range c.Snapshot.Tables {
		key := st.Schema + "." + st.Name
		pubTable, exists := pubMap[key]
		if !exists {
			return nil, fmt.Errorf(
				"snapshot table '%s' not found in publication '%s'. "+
					"For snapshot+CDC mode, snapshot.tables must be a subset of publication tables",
				key, c.Publication.Name,
			)
		}
		// Use publication table config (includes replica identity etc.)
		validatedTables = append(validatedTables, pubTable)
	}

	return validatedTables, nil
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

	// Skip CDC-related validation for snapshot_only mode
	if !c.IsSnapshotOnlyMode() {
		if cErr := c.Publication.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}

		if cErr := c.Slot.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if cErr := c.Snapshot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}

	// Heartbeat validation (applies to both CDC and snapshot_only configs;
	// feature is only used in CDC mode but we validate config consistently)
	if c.Heartbeat.Enabled {
		if c.Heartbeat.Interval <= 0 {
			err = errors.Join(err, errors.New("heartbeat.interval must be greater than 0 when heartbeat.enabled is true"))
		}
		if isEmpty(c.Heartbeat.Query) {
			err = errors.Join(err, errors.New("heartbeat.query cannot be empty when heartbeat.enabled is true"))
		}
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
	Mode              SnapshotMode       `json:"mode" yaml:"mode"`
	InstanceID        string             `json:"instanceId" yaml:"instanceId"`
	Tables            publication.Tables `json:"tables" yaml:"tables"`
	ChunkSize         int64              `json:"chunkSize" yaml:"chunkSize"`
	ClaimTimeout      time.Duration      `json:"claimTimeout" yaml:"claimTimeout"`
	HeartbeatInterval time.Duration      `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	Enabled           bool               `json:"enabled" yaml:"enabled"`
}

func (s *SnapshotConfig) Validate() error {
	if !s.Enabled {
		return nil
	}

	validModes := []SnapshotMode{SnapshotModeInitial, SnapshotModeNever, SnapshotModeSnapshotOnly}
	isValid := false
	for _, mode := range validModes {
		if s.Mode == mode {
			isValid = true
			break
		}
	}
	if !isValid {
		return errors.New("snapshot mode must be 'initial', 'never', or 'snapshot_only'")
	}

	// Validate chunk-based config
	if s.ChunkSize <= 0 {
		return errors.New("snapshot chunk size must be greater than 0")
	}
	if s.ClaimTimeout <= 0 {
		return errors.New("snapshot claim timeout must be greater than 0")
	}
	if s.HeartbeatInterval <= 0 {
		return errors.New("snapshot heartbeat interval must be greater than 0")
	}

	// For snapshot_only mode, tables must be specified
	if s.Mode == SnapshotModeSnapshotOnly && len(s.Tables) == 0 {
		return errors.New("snapshot.tables must be specified for snapshot_only mode")
	}

	return nil
}

type SnapshotMode string

const (
	SnapshotModeInitial      SnapshotMode = "initial"
	SnapshotModeNever        SnapshotMode = "never"
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
)
