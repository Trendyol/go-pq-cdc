package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

// Reproduces: unchanged TOASTed column reported as NULL on a primary-key-changing UPDATE.
// See README.md in this directory.

func main() {
	ctx := context.Background()
	cfg := config.Config{
		Host:      "127.0.0.1",
		Port:      5433,
		Username:  "cdc_user",
		Password:  "cdc_pass",
		Database:  "cdc_db",
		DebugMode: false,
		Publication: publication.Config{
			CreateIfNotExists: true,
			Name:              "cdc_publication",
			Operations: publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationUpdate,
			},
			Tables: publication.Tables{
				publication.Table{
					Name:            "docs",
					ReplicaIdentity: publication.ReplicaIdentityDefault,
					Schema:          "public",
				},
			},
		},
		Slot: slot.Config{
			CreateIfNotExists:           true,
			Name:                        "cdc_slot",
			SlotActivityCheckerInterval: 3000,
		},
		Metric: config.MetricConfig{
			Port: 8081,
		},
		Logger: config.LoggerConfig{
			LogLevel: slog.LevelInfo,
		},
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
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		slog.Info("insert message received", "new", msg.Decoded)
	case *format.Delete:
		slog.Info("delete message received", "old", msg.OldDecoded)
	case *format.Update:
		printUpdate(msg)
	}

	if err := ctx.Ack(); err != nil {
		slog.Error("ack", "error", err)
	}
}

func printUpdate(msg *format.Update) {
	newVal, newOK := msg.NewDecoded["payload"]
	oldVal, oldOK := msg.OldDecoded["payload"]

	fmt.Printf("---- UPDATE %s.%s\n", msg.TableNamespace, msg.TableName)
	fmt.Printf("  OldTupleType:        %c\n", rune(msg.OldTupleType))
	fmt.Printf("  NewDecoded[payload]: %s\n", describe(newVal, newOK))
	fmt.Printf("  OldDecoded[payload]: %s\n", describe(oldVal, oldOK))

	s, isString := newVal.(string)
	switch {
	case newOK && newVal == nil && msg.OldTupleType == 'K':
		fmt.Println("  BUG: payload reported as NULL although the UPDATE never touched it (unchanged TOAST copied from 'K' tuple)")
	case newOK && newVal == nil:
		fmt.Println("  REAL NULL: the UPDATE itself set payload = NULL")
	case !newOK:
		fmt.Println("  NOTE: payload silently absent from NewDecoded (unchanged TOAST, no old tuple)")
	case isString && len(s) == 5000:
		fmt.Println("  OK: payload carried through")
	default:
		fmt.Println("  UNEXPECTED: payload has an unrecognised value, see above")
	}
	fmt.Println()
}

func describe(v any, ok bool) string {
	if !ok {
		return "<absent>"
	}
	if v == nil {
		return "nil"
	}
	if s, isString := v.(string); isString {
		head := s
		if len(head) > 20 {
			head = head[:20] + "..."
		}
		return fmt.Sprintf("%q (len %d)", head, len(s))
	}
	return fmt.Sprintf("%v", v)
}
