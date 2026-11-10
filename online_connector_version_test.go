package cdc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Even an unqualified major must reach the actual metadata durability guard.
// This server permits no publication, slot or job mutations.
func TestOnlineDurabilityGuardWithoutMajorAllowlist(t *testing.T) {
	t.Setenv("PGSSLMODE", "disable")
	for _, version := range []int{140015, 180000, 180006, 190000} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverDone := make(chan error, 1)
			go func() { serverDone <- serveOnlineDurabilityGuard(listener, version) }()
			host, port, _ := net.SplitHostPort(listener.Addr().String())
			portNum, _ := strconv.Atoi(port)
			cfg := config.Config{Host: host, Port: portNum, Username: "u", Password: "p", Database: "db", Publication: publication.Config{Name: "p", CreateIfNotExists: true, Operations: publication.Operations{"INSERT", "UPDATE", "DELETE", "TRUNCATE"}, Tables: publication.Tables{{Schema: "public", Name: "items", ReplicaIdentity: "FULL"}}}, Slot: slot.Config{Name: "s", ProtoVersion: 1, CreateIfNotExists: true}, Snapshot: config.SnapshotConfig{Enabled: true, Mode: config.SnapshotModeInitial, Strategy: config.SnapshotStrategyOnline}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := NewOnlineConnector(ctx, cfg, func(context.Context, OnlineEvent) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			err = c.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "requires fsync=on") {
				t.Errorf("expected metadata durability failure, got: %v", err)
			}
			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("version test server timed out")
			}
		})
	}
}

func serveOnlineDurabilityGuard(listener net.Listener, version int) error {
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	backend := pgproto3.NewBackend(conn, conn)
	startup, err := backend.ReceiveStartupMessage()
	if err != nil {
		return err
	}
	if _, ssl := startup.(*pgproto3.SSLRequest); ssl {
		if _, err = conn.Write([]byte{'N'}); err != nil {
			return err
		}
		startup, err = backend.ReceiveStartupMessage()
		if err != nil {
			return err
		}
	}
	if _, ok := startup.(*pgproto3.StartupMessage); !ok {
		return fmt.Errorf("unexpected startup: %T", startup)
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: fmt.Sprintf("%d.%d", version/10000, version%10000)})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err = backend.Flush(); err != nil {
		return err
	}
	queried := false
	binaryResult := false
	for {
		message, err := backend.Receive()
		if err != nil {
			return err
		}
		switch m := message.(type) {
		case *pgproto3.Parse:
			if strings.TrimSpace(m.Query) != "SELECT current_setting('fsync')::boolean" {
				return fmt.Errorf("unexpected query before durability admission: %s", m.Query)
			}
			queried = true
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			backend.Send(&pgproto3.ParameterDescription{})
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("fsync"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1}}})
		case *pgproto3.Bind:
			binaryResult = len(m.ResultFormatCodes) > 0 && m.ResultFormatCodes[0] == 1
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			value := []byte("f")
			if binaryResult {
				value = []byte{0}
			}
			backend.Send(&pgproto3.DataRow{Values: [][]byte{value}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Terminate:
			if !queried {
				return errors.New("no durability query observed")
			}
			return nil
		default:
			return fmt.Errorf("unexpected frontend message: %T", message)
		}
		if err = backend.Flush(); err != nil {
			return err
		}
	}
}
