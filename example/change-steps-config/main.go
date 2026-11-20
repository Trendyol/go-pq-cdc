package main

import (
	"context"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"gopkg.in/yaml.v2"
)

/*
	The same idea as example/change-steps, with the steps read from config.yml.

	cd example/change-steps && docker compose up -d --wait && cd -
	cd example/change-steps-config
	go run .

	// from another terminal, in example/change-steps
	docker compose exec -T postgres psql -U cdc_user -d cdc_db <<'SQL'
	INSERT INTO users (name, email, password) VALUES ('ada', 'ada@example.com', 'secret');
	INSERT INTO orders (user_id, status, amount) VALUES (1, 'paid', 49.90);
	DELETE FROM orders WHERE id = 1;
	SQL

	// logged: the user without password, email masked, source_db added;
	// the order with customer_id; the delete is filtered out by operations.
*/

type appConfig struct {
	Tables map[string]change.Config `yaml:"tables"`
	CDC    config.Config            `yaml:"cdc"`
}

func main() {
	ctx := context.Background()

	cfg, err := readConfig("config.yml")
	if err != nil {
		slog.Error("read config", "error", err)
		os.Exit(1)
	}

	// Build each table's steps once. Steps validates the config, so a typo fails here, at startup.
	steps := make(map[string][]change.Step, len(cfg.Tables))
	for table, tableCfg := range cfg.Tables {
		tableSteps, err := tableCfg.Steps()
		if err != nil {
			slog.Error("table config", "table", table, "error", err)
			os.Exit(1)
		}
		steps[change.QualifiedName(table)] = tableSteps
	}

	connector, err := cdc.NewConnector(ctx, cfg.CDC, func(ctx *replication.ListenerContext) {
		if c, ok := change.FromListener(ctx); ok {
			c = c.Clone()
			if change.Run(c, steps[c.FullName()]...) {
				slog.Info("change", "op", c.Op, "table", c.FullName(), "old", c.Old, "new", c.New)
			}
		}

		if err := ctx.Ack(); err != nil {
			slog.Error("ack", "error", err)
		}
	})
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func readConfig(path string) (appConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return appConfig{}, err
	}
	var cfg appConfig
	err = yaml.Unmarshal(b, &cfg)
	return cfg, err
}
