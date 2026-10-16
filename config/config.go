package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

const defaultSchema = "public"

type Config struct {
	Logger           LoggerConfig          `json:"logger" yaml:"logger"`
	Host             string                `json:"host" yaml:"host"`
	Username         string                `json:"username" yaml:"username"`
	Password         string                `json:"password" yaml:"password"`
	Database         string                `json:"database" yaml:"database"`
	Publication      publication.Config    `json:"publication" yaml:"publication"`
	Heartbeat        HeartbeatConfig       `json:"heartbeat" yaml:"heartbeat"`
	Slot             slot.Config           `json:"slot" yaml:"slot"`
	VisibilityGuard  VisibilityGuardConfig `json:"visibilityGuard" yaml:"visibilityGuard"`
	Snapshot         SnapshotConfig        `json:"snapshot" yaml:"snapshot"`
	Port             int                   `json:"port" yaml:"port"`
	Metric           MetricConfig          `json:"metric" yaml:"metric"`
	DebugMode        bool                  `json:"debugMode" yaml:"debugMode"`
	ExtensionSupport ExtensionSupport      `json:"extensionSupport" yaml:"extensionSupport"`
}

// VisibilityGuardConfig holds the first message of every transaction until the
// transaction is visible to new snapshots on the primary. See
// docs/visibility-gate-design.md.
type VisibilityGuardConfig struct {
	// Replicas lists standbys (host:port) that must have replayed the
	// transaction's commit record before its first message is dispatched.
	// Credentials and database come from the main config. Direct hosts only,
	// never a pooled endpoint. See docs/replica-guard-design.md.
	FailMode     VisibilityFailMode `json:"failMode" yaml:"failMode"`
	Replicas     []string           `json:"replicas" yaml:"replicas"`
	Timeout      time.Duration      `json:"timeout" yaml:"timeout"`
	PollInterval time.Duration      `json:"pollInterval" yaml:"pollInterval"`
	Enabled      bool               `json:"enabled" yaml:"enabled"`
}

type VisibilityFailMode string

const (
	// VisibilityFailClosed restarts the stream on a timeout or guard error; the message is redelivered.
	VisibilityFailClosed VisibilityFailMode = "closed"
	// VisibilityFailOpen logs a warning and dispatches anyway on a timeout; guard errors still restart the stream.
	VisibilityFailOpen VisibilityFailMode = "open"
)

func (v *VisibilityGuardConfig) Validate() error {
	if !v.Enabled {
		if len(v.Replicas) > 0 {
			return errors.New("visibilityGuard.replicas requires visibilityGuard.enabled: true")
		}
		return nil
	}
	var err error
	seen := make(map[string]bool, len(v.Replicas))
	for _, r := range v.Replicas {
		if _, _, e := net.SplitHostPort(r); e != nil {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.replicas: %q must be host:port", r))
		}
		if seen[r] {
			err = errors.Join(err, fmt.Errorf("visibilityGuard.replicas: %q listed twice", r))
		}
		seen[r] = true
	}
	if v.FailMode != VisibilityFailClosed && v.FailMode != VisibilityFailOpen {
		err = errors.Join(err, errors.New("visibilityGuard.failMode must be 'closed' or 'open'"))
	}
	if v.Timeout <= 0 {
		err = errors.Join(err, errors.New("visibilityGuard.timeout must be greater than 0"))
	}
	if v.PollInterval <= 0 {
		err = errors.Join(err, errors.New("visibilityGuard.pollInterval must be greater than 0"))
	}
	return err
}

type MetricConfig struct {
	Port int `json:"port" yaml:"port"`
}

type LoggerConfig struct {
	Logger   logger.Logger `json:"-" yaml:"-"`         // custom logger
	LogLevel slog.Level    `json:"level" yaml:"level"` // if custom logger is nil, set the slog log level
}

type ExtensionSupport struct {
	EnableTimeScaleDB bool `json:"enableTimeScaleDB" yaml:"enableTimescaleDB"`
}

type HeartbeatConfig struct {
	Table    publication.Table `json:"table" yaml:"table"`
	Interval time.Duration     `json:"interval" yaml:"interval"`
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

// ReplicaDSN is DSN() pointed at one visibilityGuard.replicas entry (host:port).
func (c *Config) ReplicaDSN(hostPort string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s", url.QueryEscape(c.Username), url.QueryEscape(c.Password), hostPort, c.Database)
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

	// Default heartbeat interval when table is configured
	if c.Heartbeat.Table.Name != "" {
		if c.Heartbeat.Interval == 0 {
			c.Heartbeat.Interval = 100 * time.Millisecond
		}
		if c.Heartbeat.Table.Schema == "" {
			c.Heartbeat.Table.Schema = defaultSchema
		}
	}

	if c.Slot.SlotActivityCheckerInterval == 0 {
		c.Slot.SlotActivityCheckerInterval = 1000
	}

	if c.Slot.ProtoVersion == 0 {
		c.Slot.ProtoVersion = 2
	}

	if c.Logger.Logger == nil {
		c.Logger.Logger = logger.NewSlog(c.Logger.LogLevel)
	}

	if c.VisibilityGuard.Enabled {
		if c.VisibilityGuard.FailMode == "" {
			c.VisibilityGuard.FailMode = VisibilityFailClosed
		}
		if c.VisibilityGuard.Timeout == 0 {
			c.VisibilityGuard.Timeout = 10 * time.Second
		}
		if c.VisibilityGuard.PollInterval == 0 {
			c.VisibilityGuard.PollInterval = 5 * time.Millisecond
		}
	}

	// Set default schema names for tables
	for tableID, table := range c.Publication.Tables {
		if table.Schema == "" {
			c.Publication.Tables[tableID].Schema = defaultSchema
		}
	}

	c.setSnapshotStrategyDefault()

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
				c.Snapshot.Tables[tableID].Schema = defaultSchema
			}
		}
	}
}

// Omitted strategy retains the exported-snapshot implementation.
func (c *Config) setSnapshotStrategyDefault() {
	if c.Snapshot.Strategy == "" {
		c.Snapshot.Strategy = SnapshotStrategyConsistent
	}
	if c.Snapshot.Strategy == SnapshotStrategyOnline {
		c.Snapshot.Online.setDefault()
	}
}

// IsSnapshotOnlyMode returns true if snapshot is enabled and mode is snapshot_only
func (c *Config) IsSnapshotOnlyMode() bool {
	return c.Snapshot.Enabled && c.Snapshot.Mode == SnapshotModeSnapshotOnly
}

// IsHeartbeatEnabled returns true if heartbeat table is configured
func (c *Config) IsHeartbeatEnabled() bool {
	return c.Heartbeat.Table.Name != ""
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
	// Use all tables from publication with merged user config (preserves SnapshotPartitionStrategy)
	return c.mergePublicationTableConfig(publicationInfo.Tables), nil
}

// validateSnapshotSubset ensures snapshot.tables is a subset of publication tables
// and returns the validated snapshot tables with publication metadata (like replica identity)
// while preserving user's SnapshotPartitionStrategy from snapshot.tables config
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
		mergedTable := pubTable
		if st.SnapshotPartitionStrategy != "" {
			mergedTable.SnapshotPartitionStrategy = st.SnapshotPartitionStrategy
		}
		if st.QueryCondition != "" {
			mergedTable.QueryCondition = st.QueryCondition
		}
		validatedTables = append(validatedTables, mergedTable)
	}

	return validatedTables, nil
}

func (c *Config) ValidateHeartbeatInPublication(pubInfo *publication.Config) error {
	if !c.IsHeartbeatEnabled() || pubInfo == nil {
		return nil
	}

	if c.Publication.AllTables || pubInfo.AllTables {
		return nil
	}

	schema := c.Heartbeat.Table.Schema
	if schema == "" {
		schema = defaultSchema
	}
	name := c.Heartbeat.Table.Name
	if !pubInfo.Tables.Contains(schema, name) {
		return fmt.Errorf(
			"heartbeat table %s.%s is not included in publication %q; add it to publication.tables so heartbeat changes reach the replication slot",
			schema, name, pubInfo.Name,
		)
	}

	return nil
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

		if cErr := c.VisibilityGuard.Validate(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if cErr := c.Snapshot.Validate(); cErr != nil {
		err = errors.Join(err, cErr)
	}
	if c.Snapshot.Strategy == SnapshotStrategyOnline {
		err = errors.Join(err, c.validateOnline())
	}

	if c.IsHeartbeatEnabled() {
		if c.Heartbeat.Interval <= 0 {
			err = errors.Join(err, errors.New("heartbeat.interval must be greater than 0 when heartbeat table is configured"))
		}
		if !c.IsSnapshotOnlyMode() && !c.Publication.AllTables && len(c.Publication.Tables) > 0 {
			if hErr := c.ValidateHeartbeatInPublication(&publication.Config{
				Name:   c.Publication.Name,
				Tables: c.Publication.Tables,
			}); hErr != nil {
				err = errors.Join(err, hErr)
			}
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

func (c *Config) mergePublicationTableConfig(pubInfoTables publication.Tables) publication.Tables {
	if len(c.Publication.Tables) == 0 {
		return pubInfoTables
	}

	userConfigMap := make(map[string]publication.Table)
	for _, t := range c.Publication.Tables {
		key := t.Schema + "." + t.Name
		userConfigMap[key] = t
	}

	result := make(publication.Tables, len(pubInfoTables))
	for i, t := range pubInfoTables {
		result[i] = t
		key := t.Schema + "." + t.Name
		if userTable, exists := userConfigMap[key]; exists {
			if userTable.SnapshotPartitionStrategy != "" {
				result[i].SnapshotPartitionStrategy = userTable.SnapshotPartitionStrategy
			}
			if userTable.QueryCondition != "" {
				result[i].QueryCondition = userTable.QueryCondition
			}
		}
	}
	return result
}

type SnapshotConfig struct {
	Strategy          SnapshotStrategy     `json:"strategy" yaml:"strategy"`
	Online            OnlineSnapshotConfig `json:"online" yaml:"online"`
	Mode              SnapshotMode         `json:"mode" yaml:"mode"`
	InstanceID        string               `json:"instanceId" yaml:"instanceId"`
	ID                string               `json:"id" yaml:"id"`
	QueryCondition    string               `json:"queryCondition,omitempty" yaml:"queryCondition,omitempty"`
	Tables            publication.Tables   `json:"tables" yaml:"tables"`
	ChunkSize         int64                `json:"chunkSize" yaml:"chunkSize"`
	ClaimTimeout      time.Duration        `json:"claimTimeout" yaml:"claimTimeout"`
	HeartbeatInterval time.Duration        `json:"heartbeatInterval" yaml:"heartbeatInterval"`
	Enabled           bool                 `json:"enabled" yaml:"enabled"`
	Resnapshot        bool                 `json:"resnapshot" yaml:"resnapshot"`
}

func (s *SnapshotConfig) Validate() error {
	if s.Strategy != "" && s.Strategy != SnapshotStrategyConsistent && s.Strategy != SnapshotStrategyOnline {
		return errors.New("snapshot.strategy must be 'consistent' or 'online'")
	}
	if s.Strategy == SnapshotStrategyOnline {
		if !s.Enabled || s.Mode != SnapshotModeInitial {
			return errors.New("snapshot.strategy online requires snapshot.enabled: true and snapshot.mode: initial")
		}
		if s.Resnapshot {
			return errors.New("snapshot.resnapshot is unsupported with online strategy")
		}
		if err := s.Online.Validate(); err != nil {
			return err
		}
	}
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

	if s.QueryCondition != "" {
		if err := publication.ValidateQueryCondition(s.QueryCondition); err != nil {
			return err
		}
	}
	for _, t := range s.Tables {
		if t.QueryCondition != "" {
			if err := publication.ValidateQueryCondition(t.QueryCondition); err != nil {
				return fmt.Errorf("snapshot.tables %s.%s: %w", t.Schema, t.Name, err)
			}
		}
	}

	return nil
}

type SnapshotMode string

const (
	SnapshotModeInitial      SnapshotMode = "initial"
	SnapshotModeNever        SnapshotMode = "never"
	SnapshotModeSnapshotOnly SnapshotMode = "snapshot_only"
)

// SnapshotStrategy selects the snapshot implementation; consistent remains the default.
type SnapshotStrategy string

const (
	SnapshotStrategyConsistent SnapshotStrategy = "consistent"
	SnapshotStrategyOnline     SnapshotStrategy = "online"
)

// OnlineSnapshotConfig limits volatile row memory and waits in the opt-in POC.
// Byte budgets count encoded bytes, not Go heap overhead.
type OnlineSnapshotConfig struct {
	QueryTimeout       time.Duration `json:"queryTimeout" yaml:"queryTimeout"`
	DeliveryTimeout    time.Duration `json:"deliveryTimeout" yaml:"deliveryTimeout"`
	ChunkBytes         int64         `json:"chunkBytes" yaml:"chunkBytes"`
	TransactionBytes   int64         `json:"transactionBytes" yaml:"transactionBytes"`
	QueueBytes         int64         `json:"queueBytes" yaml:"queueBytes"`
	CheckpointInterval time.Duration `json:"checkpointInterval" yaml:"checkpointInterval"`
	MarkerInterval     time.Duration `json:"markerInterval" yaml:"markerInterval"`
}

func (o *OnlineSnapshotConfig) setDefault() {
	// Provisional experiment settings from the approved POC plan, not measured
	// production recommendations. Only the opt-in online strategy applies them.
	if o.QueryTimeout == 0 {
		o.QueryTimeout = 5 * time.Second
	}
	if o.DeliveryTimeout == 0 {
		o.DeliveryTimeout = 30 * time.Second
	}
	if o.ChunkBytes == 0 {
		o.ChunkBytes = 4 << 20
	}
	if o.TransactionBytes == 0 {
		o.TransactionBytes = 16 << 20
	}
	if o.QueueBytes == 0 {
		o.QueueBytes = 32 << 20
	}
	if o.CheckpointInterval == 0 {
		o.CheckpointInterval = time.Second
	}
	if o.MarkerInterval == 0 {
		o.MarkerInterval = 5 * time.Second
	}
}

func (o *OnlineSnapshotConfig) Validate() error {
	var err error
	for _, v := range []struct {
		name  string
		value int64
	}{
		{"queryTimeout", int64(o.QueryTimeout)}, {"deliveryTimeout", int64(o.DeliveryTimeout)},
		{"chunkBytes", o.ChunkBytes}, {"transactionBytes", o.TransactionBytes}, {"queueBytes", o.QueueBytes},
		{"checkpointInterval", int64(o.CheckpointInterval)}, {"markerInterval", int64(o.MarkerInterval)},
	} {
		if v.value <= 0 {
			err = errors.Join(err, fmt.Errorf("snapshot.online.%s must be greater than 0", v.name))
		}
	}
	if o.QueueBytes < o.TransactionBytes {
		err = errors.Join(err, errors.New("snapshot.online.queueBytes must admit at least one maximum transaction (transactionBytes)"))
	}
	return err
}

// validateOnline covers configuration facts only. The online constructor must
// also validate actual PG16 publication/catalog, key, schema and image facts
// for every captured business table, including tables not selected for READs.
func (c *Config) validateOnline() error {
	var err error
	reject := func(ok bool, message string) {
		if ok {
			err = errors.Join(err, errors.New(message))
		}
	}
	reject(c.Slot.ProtoVersion != 1, "snapshot.strategy online requires explicit slot.protoVersion: 1 (protocol 1); protocol 2 is not downgraded")
	reject(c.Publication.AllTables, "snapshot.strategy online requires an explicit table publication; FOR ALL TABLES is unsupported")
	reject(c.Slot.Failover, "slot.failover is unsupported in the same-primary PG16 online POC")
	reject(c.ExtensionSupport.EnableTimeScaleDB, "extensionSupport.enableTimescaleDB is unsupported in the online POC (TimescaleDB)")
	reject(len(c.VisibilityGuard.Replicas) > 0, "visibilityGuard.replicas is unsupported in the same-primary online POC")
	reject(c.VisibilityGuard.Enabled && c.VisibilityGuard.FailMode == VisibilityFailOpen, "online visibility must fail closed; visibilityGuard.failMode: open is unsupported")
	reject(c.Snapshot.QueryCondition != "", "snapshot.queryCondition is unsupported with online strategy")
	if c.Publication.CreateIfNotExists || len(c.Publication.Operations) > 0 {
		err = errors.Join(err, c.Publication.Operations.Validate())
		for _, op := range publication.OperationOptions {
			reject(!c.Publication.Operations.Contains(op), "online publication.operations requires INSERT, UPDATE, DELETE, TRUNCATE")
		}
	}
	for _, group := range []struct {
		name   string
		tables publication.Tables
	}{{"publication.tables", c.Publication.Tables}, {"snapshot.tables", c.Snapshot.Tables}} {
		for _, t := range group.tables {
			prefix := fmt.Sprintf("%s %s.%s: ", group.name, t.Schema, t.Name)
			reject(t.QueryCondition != "", prefix+"queryCondition is unsupported with online strategy")
			reject(len(t.Columns) > 0, prefix+"publication column lists are unsupported with online strategy")
			reject(t.ReplicaIdentity != "" && t.ReplicaIdentity != publication.ReplicaIdentityFull, prefix+"replicaIdentity must be FULL for verified complete online row images")
			reject(t.Partitioned, prefix+"partitioned tables are unsupported with online strategy")
			reject(t.SnapshotPartitionStrategy != publication.SnapshotPartitionStrategyAuto && t.SnapshotPartitionStrategy != publication.SnapshotPartitionStrategyIntegerRange, prefix+"snapshot partition strategy must use integer keyset scanning in online mode")
		}
	}
	return err
}
