package config

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSlotInfoTimeoutDefaults(t *testing.T) {
	cfg := Config{Slot: slot.Config{Name: "test_slot"}}
	cfg.SetDefault()
	require.Equal(t, slot.DefaultInfoTimeout, cfg.Slot.InfoTimeout)
	require.NoError(t, cfg.Slot.Validate())

	cfg.Slot.InfoTimeout = 250 * time.Millisecond
	cfg.SetDefault()
	require.Equal(t, 250*time.Millisecond, cfg.Slot.InfoTimeout)

	cfg.Slot.InfoTimeout = -time.Second
	cfg.SetDefault()
	require.ErrorContains(t, cfg.Validate(), "infoTimeout")
}

func TestSlotInfoTimeoutDecoding(t *testing.T) {
	t.Run("YAML duration string", func(t *testing.T) {
		var cfg Config
		require.NoError(t, yaml.Unmarshal([]byte("slot:\n  infoTimeout: 150ms\n"), &cfg))
		cfg.SetDefault()
		require.Equal(t, 150*time.Millisecond, cfg.Slot.InfoTimeout)
	})
	t.Run("JSON nanoseconds", func(t *testing.T) {
		var cfg Config
		require.NoError(t, json.Unmarshal([]byte(`{"slot":{"infoTimeout":150000000}}`), &cfg))
		cfg.SetDefault()
		require.Equal(t, 150*time.Millisecond, cfg.Slot.InfoTimeout)
	})
}
