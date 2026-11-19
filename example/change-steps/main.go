package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

/*
	cd example/change-steps
	docker compose up -d --wait
	go run .

	// from another terminal
	docker compose exec -T postgres psql -U cdc_user -d cdc_db <<'SQL'
	INSERT INTO users (name, email, password) VALUES ('ada', 'ada@example.com', 'secret');
	INSERT INTO orders (user_id, status, amount) VALUES (1, 'draft', 10);
	INSERT INTO orders (user_id, status, amount) VALUES (1, 'paid', 49.90);
	DELETE FROM users WHERE id = 1;
	SQL

	// logged: the user without password, email masked, name in upper case;
	// only the paid order, with user_id renamed to customer_id; the delete.
*/

// steps run on every change, in order. When applies its steps only to the changes it matches,
// so one list can treat each table differently.
var steps = []change.Step{
	change.OnlyOps(change.Insert, change.Update, change.Delete),
	change.When(change.IsTable("users"),
		change.Exclude("password"),
		change.Mask("email"),
		upperName,
	),
	change.When(change.IsTable("orders"),
		change.Where(func(c *change.Change) bool { return c.Row()["status"] != "draft" }),
		change.Rename(map[string]string{"user_id": "customer_id"}),
	),
}

// upperName is a step written in this example: any func(*change.Change) bool is a step.
func upperName(c *change.Change) bool {
	if name, ok := c.New["name"].(string); ok {
		c.New["name"] = strings.ToUpper(name)
	}
	return true
}

func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:     "127.0.0.1",
		Port:     5433,
		Username: "cdc_user",
		Password: "cdc_pass",
		Database: "cdc_db",
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication_change_steps",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationUpdate,
				publication.OperationDelete,
			},
			Tables: publication.Tables{
				{Name: "users", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
				{Name: "orders", Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot_change_steps",
			SlotActivityCheckerInterval: 3000,
		},
		Metric: config.MetricConfig{Port: 8081},
		Logger: config.LoggerConfig{LogLevel: slog.LevelInfo},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(ctx *replication.ListenerContext) {
	// Clone so the steps do not change the message the connector holds.
	if c, ok := change.FromListener(ctx); ok {
		c = c.Clone()
		if change.Run(c, steps...) {
			slog.Info("change", "op", c.Op, "table", c.FullName(), "old", c.Old, "new", c.New)
		}
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}
