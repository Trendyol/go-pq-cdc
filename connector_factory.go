package cdc

import (
	"context"
	"errors"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

type connectorConstruction struct {
	listener      replication.ListenerFunc
	onlineHandler OnlineHandler
	onlineOptions []OnlineConnectorOption
	legacySet     bool
	onlineSet     bool
}

// ConnectorOption selects a typed delivery contract for the additive factory.
// Legacy listeners and online durable handlers cannot be combined.
type ConnectorOption func(*connectorConstruction) error

// WithLegacyListener keeps the existing callback and explicit Ack contract.
func WithLegacyListener(listener replication.ListenerFunc) ConnectorOption {
	return func(options *connectorConstruction) error {
		if options.legacySet || options.onlineSet {
			return errors.New("connector accepts exactly one delivery handler option")
		}
		options.legacySet, options.listener = true, listener
		return nil
	}
}

// WithOnlineHandler requires synchronous durable completion. Nil from handler
// must certify completion of the destination effect, not successful enqueue.
func WithOnlineHandler(handler OnlineHandler, onlineOptions ...OnlineConnectorOption) ConnectorOption {
	owned := append([]OnlineConnectorOption(nil), onlineOptions...)
	return func(options *connectorConstruction) error {
		if options.legacySet || options.onlineSet {
			return errors.New("connector accepts exactly one delivery handler option")
		}
		options.onlineSet, options.onlineHandler = true, handler
		options.onlineOptions = owned
		return nil
	}
}

// NewConnectorWithOptions selects a backend from snapshot.strategy without
// changing the existing NewConnector or NewConnectorWithConfigFile signatures.
// Online construction is network-free; Start performs admission and capture.
func NewConnectorWithOptions(ctx context.Context, cfg config.Config, options ...ConnectorOption) (Connector, error) {
	construction := connectorConstruction{}
	for _, option := range options {
		if option != nil {
			if err := option(&construction); err != nil {
				return nil, err
			}
		}
	}
	if cfg.Snapshot.Strategy != config.SnapshotStrategyOnline {
		if construction.onlineSet {
			return nil, errors.New("WithOnlineHandler requires snapshot.strategy online")
		}
		if !construction.legacySet {
			return nil, errors.New("consistent strategy requires WithLegacyListener")
		}
		return newLegacyConnector(ctx, cfg, construction.listener)
	}
	if construction.legacySet {
		return nil, errors.New("online strategy cannot use a legacy listener; use WithOnlineHandler through NewConnectorWithOptions or NewOnlineConnector")
	}
	if !construction.onlineSet {
		return nil, errors.New("online strategy requires WithOnlineHandler through NewConnectorWithOptions or NewOnlineConnector")
	}
	backend, err := newOnlineConnector(ctx, cfg, construction.onlineHandler, construction.onlineOptions...)
	if err != nil {
		return nil, err
	}
	return newOnlineConnectorAdapter(backend), nil
}
