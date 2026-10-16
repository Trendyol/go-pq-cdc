package integration

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineFullImages(t *testing.T) {
	for _, operation := range []string{"toast", "truncate", "schema", "key"} {
		t.Run(operation, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			dest := newOnlinePGFixture(t)
			conn := source.Connect(t)
			target := newOnlineTarget(t, dest)
			// MD5 concatenation avoids compression and forces an unchanged external TOAST
			// datum, not merely a highly compressible long string.
			onlineExec(t, conn, `CREATE TABLE images(id bigint PRIMARY KEY,value text,toast text,nullable text); INSERT INTO images SELECT 1,'old',string_agg(md5(i::text),'') ,NULL FROM generate_series(1,4000) i`)
			provisionOnline(t, conn, "images")
			cfg := onlineConfig(t, source, "images")
			r := startOnline(t, cfg, func(ctx context.Context, e online.Event) error { return target.Apply(ctx, e, onlineTargetOK) })
			waitOnlineCapture(t, r)
			waitOnlineSnapshot(t, r)
			switch operation {
			case "toast":
				onlineExec(t, conn, "UPDATE images SET value='new',nullable='filled' WHERE id=1; UPDATE images SET nullable=NULL WHERE id=1")
				waitOnlineFence(t, conn)
				r.Cancel()
				require.ErrorIs(t, onlineResult(t, r), context.Canceled)
				compareOnlineTable(t, conn, target, "images")
				image, deleted, _ := target.Image(t, online.Table{Schema: "public", Name: "images"}, 1)
				require.False(t, deleted)
				require.Equal(t, "new", image["value"])
				require.Nil(t, image["nullable"])
				require.Len(t, image["toast"].(string), 128000)
			case "truncate":
				onlineExec(t, conn, "TRUNCATE images")
			case "schema":
				onlineExec(t, conn, "ALTER TABLE images ADD COLUMN added text; UPDATE images SET value='changed'")
			case "key":
				onlineExec(t, conn, "UPDATE images SET id=2 WHERE id=1")
			}
			if operation != "toast" {
				require.Error(t, onlineResult(t, r))
				var unsafe bool
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
				require.False(t, unsafe)
			}
		})
	}
}
