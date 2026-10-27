package replication

import (
	"context"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseServerMajorVersion(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "major and minor", raw: "16.15", want: 16},
		{name: "pre 10 three part", raw: "9.6.24", want: 9},
		{name: "major only", raw: "17", want: 17},
		{name: "vendor suffix", raw: "14.9 (Debian 14.9-1.pgdg110+1)", want: 14},
		{name: "prerelease marker", raw: "18beta1", want: 18},
		{name: "trailing dot", raw: "16.", want: 16},
		{name: "empty", raw: "", wantErr: true},
		{name: "not numeric", raw: "unknown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseServerMajorVersion(tt.raw)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestServerMajorVersionReadsStartupParameterWithoutQuery(t *testing.T) {
	conn := &versionConn{serverVersion: "16.15"}

	got, err := New(conn).serverMajorVersion()

	require.NoError(t, err)
	assert.Equal(t, 16, got)
	assert.Zero(t, conn.execCalls, "server version must come from the startup packet, not a query")
}

func TestServerMajorVersionFailsWhenServerReportsNoVersion(t *testing.T) {
	conn := &versionConn{}

	got, err := New(conn).serverMajorVersion()

	require.Error(t, err)
	assert.Zero(t, got, "a failed version read must not report a usable version")
	assert.Zero(t, conn.execCalls, "a missing version must not trigger a query either")
}

func TestServerMajorVersionFailsWithoutStartupParameters(t *testing.T) {
	conn := struct{ pq.Connection }{}

	got, err := New(conn).serverMajorVersion()

	require.Error(t, err, "a connection that hides the startup packet must report it, not query")
	assert.Zero(t, got)
}

// versionConn exposes only the startup parameters and counts queries, so a
// version read that still went to the server shows up as a non-zero execCalls.
type versionConn struct {
	serverVersion string
	execCalls     int
}

func (c *versionConn) Connect(context.Context) error { return nil }

func (c *versionConn) IsClosed() bool { return false }

func (c *versionConn) Close(context.Context) error { return nil }

func (c *versionConn) ReceiveMessage(context.Context) (pgproto3.BackendMessage, error) {
	return nil, nil
}

func (c *versionConn) Frontend() *pgproto3.Frontend { return nil }

func (c *versionConn) Exec(context.Context, string) *pgconn.MultiResultReader {
	c.execCalls++
	return nil
}

func (c *versionConn) ParameterStatus(name string) string {
	if name == "server_version" {
		return c.serverVersion
	}
	return ""
}
