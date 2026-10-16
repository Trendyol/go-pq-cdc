// Opt-in POC demonstration. This ordered PostgreSQL target is not a production
// adapter, an exactly-once transport, or a promise of source WAL outage isolation.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/jackc/pgx/v5"
)

const targetSQL = `CREATE TABLE IF NOT EXISTS online_event_ledger(event_id text PRIMARY KEY,event_hash text NOT NULL,event jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS online_projection(table_schema text NOT NULL,table_name text NOT NULL,row_key bigint NOT NULL,image jsonb,deleted boolean NOT NULL,order_lsn pg_lsn NOT NULL,phase smallint NOT NULL,ordinal bigint NOT NULL,PRIMARY KEY(table_schema,table_name,row_key));`

func apply(ctx context.Context, conn *pgx.Conn, event online.Event) (err error) {
	if err := event.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	image, err := json.Marshal(event.Image)
	if err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, online.RollbackBounded(tx)) }()
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit=on"); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, "INSERT INTO online_event_ledger VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING", event.ID, digest, string(data))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		var existing string
		if err = tx.QueryRow(ctx, "SELECT event_hash FROM online_event_ledger WHERE event_id=$1", event.ID).Scan(&existing); err != nil {
			return err
		}
		if existing != digest {
			return errors.New("conflicting event identity")
		}
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO online_projection VALUES($1,$2,$3,$4::jsonb,$5,$6::pg_lsn,$7,$8) ON CONFLICT(table_schema,table_name,row_key) DO UPDATE SET image=EXCLUDED.image,deleted=EXCLUDED.deleted,order_lsn=EXCLUDED.order_lsn,phase=EXCLUDED.phase,ordinal=EXCLUDED.ordinal WHERE (online_projection.order_lsn,online_projection.phase,online_projection.ordinal)<(EXCLUDED.order_lsn,EXCLUDED.phase,EXCLUDED.ordinal)`, event.Table.Schema, event.Table.Name, event.Key, string(image), event.Operation == online.Delete, event.Order.EndLSN.String(), int16(event.Order.Phase), int64(event.Order.Ordinal))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func run() error {
	configFile := flag.String("config", "config.yaml", "online source YAML")
	printSQL := flag.Bool("print-source-sql", false, "print metadata migration, then exit")
	role := flag.String("role", "", "role for metadata grants in printed SQL")
	setupTarget := flag.Bool("setup-target", false, "provision target fixture tables, then exit")
	flag.Parse()
	if *printSQL {
		sql, err := online.ProvisionSQL(*role)
		if err != nil {
			return err
		}
		fmt.Print(sql)
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	targetDSN := os.Getenv("CDC_ONLINE_TARGET_DSN")
	if targetDSN == "" {
		return errors.New("CDC_ONLINE_TARGET_DSN is required (independent durable target cluster)")
	}
	target, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer target.Close(context.Background())
	var durable bool
	if err = target.QueryRow(ctx, "SELECT current_setting('fsync')='on' AND NOT pg_is_in_recovery()").Scan(&durable); err != nil {
		return err
	}
	if !durable {
		return errors.New("target requires primary fsync=on")
	}
	if *setupTarget {
		_, err = target.Exec(ctx, targetSQL)
		return err
	}
	var permanent bool
	if err = target.QueryRow(ctx, `SELECT count(*)=2 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('online_event_ledger','online_projection') AND c.relkind='r' AND c.relpersistence='p'`).Scan(&permanent); err != nil {
		return err
	}
	if !permanent {
		return errors.New("target requires permanent ledger/projection tables")
	}
	cfg, err := config.ReadConfigYAML(*configFile)
	if err != nil {
		return err
	}
	connector, err := cdc.NewOnlineConnector(ctx, cfg, func(ctx context.Context, e cdc.OnlineEvent) error { return apply(ctx, target, e) })
	if err != nil {
		return err
	}
	return connector.Run(ctx)
}
func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
