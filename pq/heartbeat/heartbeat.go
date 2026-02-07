package heartbeat

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
)

// quoteIdentifier quotes a PostgreSQL identifier (schema, table, column name)
// to handle reserved words, special characters, and embedded quotes safely.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Heartbeat manages periodic heartbeat updates to prevent WAL bloat
type Heartbeat struct {
	conn   pq.Connection
	cfg    config.HeartbeatConfig
	dsn    string
	mu     sync.Mutex
	closed bool
}

// New creates a new Heartbeat instance
func New(dsn string, cfg config.HeartbeatConfig) *Heartbeat {
	return &Heartbeat{
		dsn: dsn,
		cfg: cfg,
	}
}

// EnsureTable creates the heartbeat table if it doesn't exist
func (h *Heartbeat) EnsureTable(ctx context.Context, conn pq.Connection) error {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	constraint := quoteIdentifier(h.cfg.Table.Name + "_single_row")

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id INTEGER PRIMARY KEY DEFAULT 1,
			last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			CONSTRAINT %s CHECK (id = 1)
		)`, schema, table, constraint)

	insertRowSQL := fmt.Sprintf(`
		INSERT INTO %s.%s (id) VALUES (1) ON CONFLICT DO NOTHING`, schema, table)

	// Execute CREATE TABLE
	resultReader := conn.Exec(ctx, createTableSQL)
	if resultReader == nil {
		return fmt.Errorf("create heartbeat table: exec returned nil")
	}
	if _, err := resultReader.ReadAll(); err != nil {
		_ = resultReader.Close()
		return fmt.Errorf("create heartbeat table failed: %w", err)
	}
	if err := resultReader.Close(); err != nil {
		return fmt.Errorf("close create table result: %w", err)
	}

	// Execute INSERT initial row
	resultReader = conn.Exec(ctx, insertRowSQL)
	if resultReader == nil {
		return fmt.Errorf("insert heartbeat row: exec returned nil")
	}
	if _, err := resultReader.ReadAll(); err != nil {
		_ = resultReader.Close()
		return fmt.Errorf("insert heartbeat row failed: %w", err)
	}
	if err := resultReader.Close(); err != nil {
		return fmt.Errorf("close insert result: %w", err)
	}

	logger.Info("heartbeat table created", "table", schema+"."+table)
	return nil
}

// Run starts the heartbeat loop. It blocks until context is cancelled.
func (h *Heartbeat) Run(ctx context.Context) {
	logger.Debug("heartbeat loop started",
		"interval", h.cfg.Interval,
		"table", h.cfg.Table.Schema+"."+h.cfg.Table.Name)

	ticker := time.NewTicker(h.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("heartbeat loop stopped", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := h.execute(ctx); err != nil {
				logger.Error("heartbeat execution failed", "error", err)
			} else {
				logger.Debug("heartbeat query executed")
			}
		}
	}
}

// execute runs a single heartbeat update
func (h *Heartbeat) execute(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil
	}

	// Lazily (re)establish connection if needed
	if h.conn == nil {
		conn, err := pq.NewConnection(ctx, h.dsn)
		if err != nil {
			return fmt.Errorf("heartbeat connection (re)establish failed: %w", err)
		}
		h.conn = conn
	}

	query := h.query()
	resultReader := h.conn.Exec(ctx, query)
	if resultReader == nil {
		return fmt.Errorf("heartbeat exec returned nil resultReader")
	}
	defer func() {
		if err := resultReader.Close(); err != nil {
			logger.Error("heartbeat result reader close failed", "error", err)
		}
	}()

	if _, err := resultReader.ReadAll(); err != nil {
		// On error, proactively close and nil the connection so that the next
		// heartbeat tick will try to re-establish it.
		_ = h.conn.Close(ctx)
		h.conn = nil
		return fmt.Errorf("heartbeat query failed: %w", err)
	}

	return nil
}

// query returns the auto-generated UPDATE query for heartbeat
func (h *Heartbeat) query() string {
	schema := quoteIdentifier(h.cfg.Table.Schema)
	table := quoteIdentifier(h.cfg.Table.Name)
	return fmt.Sprintf(`UPDATE %s.%s SET last_heartbeat = NOW() WHERE id = 1`, schema, table)
}

// Close closes the heartbeat connection
func (h *Heartbeat) Close(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.closed = true
	if h.conn != nil {
		_ = h.conn.Close(ctx)
		h.conn = nil
	}
}
