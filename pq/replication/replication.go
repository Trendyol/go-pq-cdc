package replication

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
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

// StartOptions carries the pgoutput options that shape START_REPLICATION.
type StartOptions struct {
	// ProtoVersion selects the pgoutput protocol version.
	ProtoVersion int
	// LogicalMessages selects the logical decoding message policy; its zero
	// value (auto) requests the option whenever the server supports it.
	LogicalMessages slot.LogicalMessagesMode
}

// Start begins replication with the default pgoutput options for protoVersion.
// Callers that need to override an option use StartWithOptions; this signature
// is kept unchanged for compatibility.
func (r *Replication) Start(publicationName, slotName string, startLSN pq.LSN, protoVersion int) error {
	return r.StartWithOptions(publicationName, slotName, startLSN, StartOptions{ProtoVersion: protoVersion})
}

// StartWithOptions begins replication with explicit pgoutput options.
func (r *Replication) StartWithOptions(publicationName, slotName string, startLSN pq.LSN, opts StartOptions) error {
	serverMajorVersion, versionKnown := r.serverMajorVersion()

	pluginArguments, err := replicationPluginArguments(opts, serverMajorVersion, versionKnown)
	if err != nil {
		return err
	}
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
// packet. A missing or unreadable value is unknown rather than an error:
// only an explicit logical-message request needs the version, and that policy
// decides what an unknown version means.
func (r *Replication) serverMajorVersion() (int, bool) {
	reporter, ok := r.conn.(serverVersionReporter)
	if !ok {
		return 0, false
	}

	raw := reporter.ParameterStatus("server_version")
	if raw == "" {
		return 0, false
	}

	major, err := parseServerMajorVersion(raw)
	if err != nil {
		return 0, false
	}

	return major, true
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
// messages is requested. That option is independent of streaming and exists on
// proto version 1, but PostgreSQL 13 and older reject it.
func replicationPluginArguments(opts StartOptions, serverMajorVersion int, versionKnown bool) ([]string, error) {
	requestMessages, err := opts.requestLogicalMessages(serverMajorVersion, versionKnown)
	if err != nil {
		return nil, err
	}

	args := []string{
		fmt.Sprintf("proto_version '%d'", opts.ProtoVersion),
	}
	if requestMessages {
		args = append(args, "messages 'true'")
	}
	if opts.ProtoVersion >= 2 {
		args = append(args, "streaming 'true'")
	}
	return args, nil
}

// requestLogicalMessages resolves the logical message policy against the
// server. An explicit request the server cannot honour fails startup rather
// than being dropped silently, which would look like the feature is broken.
// Auto falls back to the protocol version when the server version is unknown,
// so a proxy or custom connection does not break a caller that never opted in.
func (o StartOptions) requestLogicalMessages(serverMajorVersion int, versionKnown bool) (bool, error) {
	supported := versionKnown && serverMajorVersion >= logicalMessagesMinMajorVersion

	switch o.LogicalMessages {
	case slot.LogicalMessagesOff:
		return false, nil
	case slot.LogicalMessagesOn:
		if !versionKnown {
			return false, errors.New("logical messages require a known server version")
		}
		if !supported {
			return false, errors.Newf(
				"logical messages require PostgreSQL %d or newer, but the server is %d",
				logicalMessagesMinMajorVersion, serverMajorVersion,
			)
		}
		return true, nil
	default: // LogicalMessagesAuto
		if !versionKnown {
			return o.ProtoVersion >= 2, nil
		}
		return supported, nil
	}
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
