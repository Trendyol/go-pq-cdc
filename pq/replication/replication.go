package replication

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	libpq "github.com/lib/pq"
)

// logicalMessagesMinMajorVersion is the first server major version that
// accepts the pgoutput messages option. PostgreSQL 10-13 reject it as
// unrecognized.
const logicalMessagesMinMajorVersion = 14

type Replication struct {
	conn pq.Connection
}

func New(conn pq.Connection) *Replication {
	return &Replication{conn: conn}
}

func (r *Replication) Start(publicationName, slotName string, startLSN pq.LSN, protoVersion int) error {
	serverMajorVersion, err := r.serverMajorVersion()
	if err != nil {
		return err
	}

	pluginArguments := replicationPluginArguments(protoVersion, serverMajorVersion)
	pluginArguments = append(pluginArguments, "publication_names '"+publicationName+"'")

	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s)", libpq.QuoteIdentifier(slotName), startLSN, strings.Join(pluginArguments, ","))
	r.conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	err = r.conn.Frontend().Flush()
	if err != nil {
		return errors.Wrap(err, "start replication")
	}
	return nil
}

// serverVersionReporter is satisfied by connections that expose the startup
// packet's parameters. pq's own connection embeds *pgconn.PgConn, which does.
type serverVersionReporter interface {
	ParameterStatus(name string) string
}

// serverMajorVersion reports the server's major version from the startup
// packet. Startup still fails when the value is missing: without it we cannot
// tell whether messages is safe to request.
func (r *Replication) serverMajorVersion() (int, error) {
	reporter, ok := r.conn.(serverVersionReporter)
	if !ok {
		return 0, errors.New("server version: connection does not expose startup parameters")
	}

	raw := reporter.ParameterStatus("server_version")
	if raw == "" {
		return 0, errors.New("server version: server did not report server_version")
	}

	return parseServerMajorVersion(raw)
}

// parseServerMajorVersion takes the leading major version from the startup
// packet's server_version, the same way pgx reads it (serverVersion in
// pgx/v5/derived_types.go). A major version is all the gate below needs, and it
// sidesteps the two-part/three-part numbering switch in PostgreSQL 10 along
// with vendor suffixes and prerelease markers such as "18beta1".
func parseServerMajorVersion(raw string) (int, error) {
	end := 0
	for end < len(raw) && raw[end] >= '0' && raw[end] <= '9' {
		end++
	}

	if end == 0 {
		return 0, errors.Newf("server version %q: no leading major version", raw)
	}

	major, err := strconv.Atoi(raw[:end])
	if err != nil {
		return 0, errors.Wrapf(err, "server version %q", raw)
	}

	return major, nil
}

// replicationPluginArguments builds the pgoutput options for START_REPLICATION.
// Logical decoding messages (pg_logical_emit_message) are sent only when
// messages is requested. That option is independent of streaming and exists
// on proto version 1, but PostgreSQL 13 and older reject it.
func replicationPluginArguments(protoVersion, serverMajorVersion int) []string {
	args := []string{
		fmt.Sprintf("proto_version '%d'", protoVersion),
	}
	if serverMajorVersion >= logicalMessagesMinMajorVersion {
		args = append(args, "messages 'true'")
	}
	if protoVersion >= 2 {
		args = append(args, "streaming 'true'")
	}
	return args
}

func (r *Replication) Test(ctx context.Context) error {
	var (
		nextTli         int64
		nextTliStartPos pq.LSN
	)
	for {
		msg, err := r.conn.ReceiveMessage(ctx)
		if err != nil {
			return errors.Newf("failed to receive message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.NoticeResponse:
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.CopyBothResponse:
			return nil
		case *pgproto3.RowDescription:
			return errors.Newf("received row RowDescription message in logical replication")
		case *pgproto3.DataRow:
			if cnt := len(msg.Values); cnt != 2 {
				return errors.Newf("expected next_tli and next_tli_startpos, got %d fields", cnt)
			}
			tmpNextTli, tmpNextTliStartPos := string(msg.Values[0]), string(msg.Values[1])
			nextTli, err = strconv.ParseInt(tmpNextTli, 10, 64)
			if err != nil {
				return err
			}
			nextTliStartPos, err = pq.ParseLSN(tmpNextTliStartPos)
			if err != nil {
				return err
			}
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			if nextTli > 0 && nextTliStartPos > 0 {
				return errors.New("start replication with a switch point")
			}
		default:
			return errors.Newf("unexpected response type: %T", msg)
		}
	}
}
