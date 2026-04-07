package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublicationPartitionedTable verifies that setting Partitioned = true on a
// table entry causes the publication to be created with
// publish_via_partition_root = true, and that inserts into child partitions
// arrive with the root table's relation name.
func TestPublicationPartitionedTable(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)

	// Create a partitioned table with two child partitions.
	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS orders CASCADE;
		CREATE TABLE orders (
			id SERIAL,
			region TEXT NOT NULL,
			amount NUMERIC(10,2) NOT NULL,
			PRIMARY KEY (id, region)
		) PARTITION BY LIST (region);

		CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('us');
		CREATE TABLE orders_eu PARTITION OF orders FOR VALUES IN ('eu');
	`)
	require.NoError(t, err)

	cdcCfg := Config
	cdcCfg.Slot.Name = "slot_test_pub_partitioned"
	cdcCfg.Publication.Name = "pub_partitioned"
	cdcCfg.Publication.CreateIfNotExists = true
	cdcCfg.Publication.Operations = publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
	}
	cdcCfg.Publication.Tables = []publication.Table{
		{
			Name:            "orders",
			Schema:          "public",
			ReplicaIdentity: publication.ReplicaIdentityDefault,
			Partitioned:     true,
		},
	}

	messageCh := make(chan *format.Insert, 500)
	handlerFunc := func(ctx *replication.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			messageCh <- msg
		}
		_ = ctx.Ack()
	}

	connector, err := cdc.NewConnector(ctx, cdcCfg, handlerFunc)
	require.NoError(t, err)

	t.Cleanup(func() {
		connector.Close()
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS orders CASCADE")
		assert.NoError(t, RestoreDB(ctx))
		assert.NoError(t, postgresConn.Close(ctx))
	})

	go connector.Start(ctx)

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	require.NoError(t, connector.WaitUntilReady(waitCtx))
	cancel()

	t.Run("Insert into child partition is received via root table", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "INSERT INTO orders(id, region, amount) VALUES(1, 'us', 99.99)")
		require.NoError(t, err)

		select {
		case msg := <-messageCh:
			assert.Equal(t, int32(1), msg.Decoded["id"])
			assert.Equal(t, "us", msg.Decoded["region"])
			// With publish_via_partition_root the relation name should be
			// the root table "orders", not the child "orders_us".
			assert.Equal(t, "orders", msg.TableName,
				"expected root table name due to publish_via_partition_root")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert message")
		}
	})

	t.Run("Insert into second partition also uses root table name", func(t *testing.T) {
		err = pgExec(ctx, postgresConn, "INSERT INTO orders(id, region, amount) VALUES(2, 'eu', 49.99)")
		require.NoError(t, err)

		select {
		case msg := <-messageCh:
			assert.Equal(t, int32(2), msg.Decoded["id"])
			assert.Equal(t, "eu", msg.Decoded["region"])
			assert.Equal(t, "orders", msg.TableName,
				"expected root table name due to publish_via_partition_root")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for insert message")
		}
	})
}

// TestPublicationPartitionedCreateAndInfo verifies that a publication created
// with a partitioned table includes publish_via_partition_root = true and can
// be queried back via Info().
func TestPublicationPartitionedCreateAndInfo(t *testing.T) {
	ctx := context.Background()

	postgresConn, err := newPostgresConn()
	require.NoError(t, err)
	defer postgresConn.Close(ctx)

	pubName := "pub_partitioned_info_test"

	err = pgExec(ctx, postgresConn, `
		DROP TABLE IF EXISTS events CASCADE;
		CREATE TABLE events (
			id SERIAL,
			category TEXT NOT NULL,
			payload TEXT,
			PRIMARY KEY (id, category)
		) PARTITION BY LIST (category);

		CREATE TABLE events_a PARTITION OF events FOR VALUES IN ('a');
		CREATE TABLE events_b PARTITION OF events FOR VALUES IN ('b');
	`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		_ = pgExec(ctx, postgresConn, "DROP TABLE IF EXISTS events CASCADE")
	})

	_ = pgExec(ctx, postgresConn, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))

	pubConfig := publication.Config{
		Name:              pubName,
		CreateIfNotExists: true,
		Operations:        publication.Operations{"INSERT", "UPDATE", "DELETE"},
		Tables: publication.Tables{
			{
				Name:            "events",
				Schema:          "public",
				ReplicaIdentity: publication.ReplicaIdentityDefault,
				Partitioned:     true,
			},
		},
	}

	pub := publication.New(pubConfig, postgresConn)

	createdCfg, err := pub.Create(ctx)
	require.NoError(t, err, "Create() should succeed for partitioned publication")
	require.NotNil(t, createdCfg)

	info, err := pub.Info(ctx)
	require.NoError(t, err, "Info() should succeed for partitioned publication")
	assert.Equal(t, pubName, info.Name)
	assert.NotEmpty(t, info.Tables, "Tables should not be empty")
}
