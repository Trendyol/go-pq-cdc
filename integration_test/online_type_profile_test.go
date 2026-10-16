package integration

import (
	"context"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	"github.com/stretchr/testify/require"
)

func TestOnlineTypeProfileStartup(t *testing.T) {
	source := newOnlinePGFixture(t)
	conn := source.Connect(t)
	require.NoError(t, online.Provision(context.Background(), conn, ""))
	onlineExec(t, conn, `CREATE DOMAIN unqualified_domain AS bigint; CREATE TYPE unqualified_enum AS ENUM('one'); CREATE TYPE unqualified_composite AS (v bigint)`)
	for _, kind := range []string{"unqualified_domain", "unqualified_enum", "unqualified_composite", "numeric", "timestamp", "timestamptz", "date", "bytea", "uuid", "int8range", "integer[][]"} {
		t.Run(kind, func(t *testing.T) {
			onlineExec(t, conn, "CREATE TABLE types(id bigint PRIMARY KEY,value "+kind+"); ALTER TABLE types REPLICA IDENTITY FULL")
			cfg := onlineConfig(t, source, "types")
			calls := 0
			connector, err := cdc.NewOnlineConnector(context.Background(), cfg, func(context.Context, online.Event) error { calls++; return nil })
			require.NoError(t, err)
			require.ErrorIs(t, connector.Run(context.Background()), online.ErrSchema)
			require.Zero(t, calls)
			var slots, jobs int
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots").Scan(&slots))
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT count(*) FROM cdc_online.jobs").Scan(&jobs))
			require.Zero(t, slots, "unqualified type created capture slot")
			require.Zero(t, jobs, "unqualified type created job")
			onlineExec(t, conn, "DROP PUBLICATION online_pub; DROP TABLE types")
		})
	}
}
func TestOnlineQualifiedArrays(t *testing.T) {
	for _, mode := range []string{"positive", "READ-multidimensional", "READ-lower-bound", "CDC-multidimensional", "CDC-lower-bound"} {
		t.Run(mode, func(t *testing.T) {
			source := newOnlinePGFixture(t)
			dest := newOnlinePGFixture(t)
			conn := source.Connect(t)
			target := newOnlineTarget(t, dest)
			onlineExec(t, conn, `CREATE TABLE arrays(id bigint PRIMARY KEY,ints integer[],text_values text[],json_values jsonb[],floats double precision[],booleans boolean[],smallints smallint[],bigints bigint[],varchar_values varchar(20)[],char_values char(3)[],real_values real[],json_plain json[],b boolean,si smallint,ii integer,bi bigint,t text,v varchar(20),c char(3),r real,d double precision,j json,jb jsonb)`)
			provisionOnline(t, conn, "arrays")
			if mode == "positive" {
				onlineExec(t, conn, `INSERT INTO arrays(id,ints,text_values,json_values,floats,booleans,smallints,bigints,varchar_values,char_values,real_values,json_plain,b,si,ii,bi,t,v,c,r,d,j,jb) VALUES(1,ARRAY[1,NULL,3],ARRAY['{brace,comma}','quoted " value'],ARRAY['{"n":9007199254740993}'::jsonb],ARRAY['Infinity'::float8,'NaN'::float8],ARRAY[true,NULL,false],ARRAY[-32768,32767]::smallint[],ARRAY[-9223372036854775808,9223372036854775807]::bigint[],ARRAY['{v,}','varchar']::varchar(20)[],ARRAY['x','y']::char(3)[],ARRAY[1.25,'Infinity'::real],ARRAY['{"n":9007199254740995,"s":"{,}"}'::json],true,-32768,2147483647,9223372036854775807,'text {,}','varchar','x',1.25,1.5,'{"n":9007199254740997}','{"n":9007199254740999}'); INSERT INTO arrays(id,ints,text_values,json_values,floats,booleans,smallints,bigints,varchar_values,char_values,real_values,json_plain) VALUES(2,'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'); INSERT INTO arrays(id) VALUES(3)`)
			}
			if mode == "READ-multidimensional" {
				onlineExec(t, conn, `INSERT INTO arrays(id,ints) VALUES(1,ARRAY[[1,2],[3,4]])`)
			}
			if mode == "READ-lower-bound" {
				onlineExec(t, conn, `INSERT INTO arrays(id,ints) VALUES(1,'[0:1]={1,2}'::integer[])`)
			}
			cfg := onlineConfig(t, source, "arrays")
			reads := 0
			r := startOnline(t, cfg, func(ctx context.Context, event online.Event) error {
				reads++
				return target.Apply(ctx, event, onlineTargetOK)
			})
			if mode == "positive" {
				waitOnlineSnapshot(t, r)
				waitOnlineFence(t, conn)
				compareOnlineTable(t, conn, target, "arrays")
				onlineExec(t, conn, `UPDATE arrays SET ints=ARRAY[4,5],json_values=ARRAY['{"n":9007199254740995}'::jsonb],floats=ARRAY['-Infinity'::float8] WHERE id=1`)
				waitOnlineFence(t, conn)
				r.Cancel()
				require.ErrorIs(t, onlineResult(t, r), context.Canceled)
				compareOnlineTable(t, conn, target, "arrays")
				return
			}
			if mode == "CDC-multidimensional" || mode == "CDC-lower-bound" {
				waitOnlineSnapshot(t, r)
				if mode == "CDC-multidimensional" {
					onlineExec(t, conn, `INSERT INTO arrays(id,ints) VALUES(1,ARRAY[[1,2],[3,4]])`)
				} else {
					onlineExec(t, conn, `INSERT INTO arrays(id,ints) VALUES(1,'[0:1]={1,2}'::integer[])`)
				}
			}
			require.Error(t, onlineResult(t, r))
			require.Zero(t, reads, "unqualified array value was delivered")
			var unsafe bool
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT s.confirmed_flush_lsn>j.source_lsn FROM pg_replication_slots s JOIN cdc_online.jobs j ON s.slot_name=j.slot_name").Scan(&unsafe))
			require.False(t, unsafe)
		})
	}
}
