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

// logicalMessagesMinServerVersion is the first server_version_num that accepts
// the pgoutput messages option. PostgreSQL 10–13 reject it as unrecognized.
const logicalMessagesMinServerVersion = 140000

type Replication struct {
	conn pq.Connection
}

func New(conn pq.Connection) *Replication {
	return &Replication{conn: conn}
}

func (r *Replication) Start(publicationName, slotName string, startLSN pq.LSN, protoVersion int) error {
	return r.start(context.Background(), publicationName, slotName, startLSN, protoVersion)
}

func (r *Replication) start(ctx context.Context, publicationName, slotName string, startLSN pq.LSN, protoVersion int) error {
	serverVersionNum, err := r.serverVersionNum(ctx)
	if err != nil {
		return err
	}

	pluginArguments := replicationPluginArguments(protoVersion, serverVersionNum)
	pluginArguments = append(pluginArguments, "publication_names '"+publicationName+"'")

	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s)", libpq.QuoteIdentifier(slotName), startLSN, strings.Join(pluginArguments, ","))
	r.conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	err = r.conn.Frontend().Flush()
	if err != nil {
		return errors.Wrap(err, "start replication")
	}
	return nil
}

// serverVersionNum reads SHOW server_version_num. Startup fails when the
// version cannot be read: without it we cannot tell whether messages is safe
// to request.
func (r *Replication) serverVersionNum(ctx context.Context) (int, error) {
	reader := r.conn.Exec(ctx, "SHOW server_version_num")
	results, err := reader.ReadAll()
	closeErr := reader.Close()
	if err != nil {
		return 0, errors.Wrap(err, "server version")
	}
	if closeErr != nil {
		return 0, errors.Wrap(closeErr, "server version")
	}
	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return 0, errors.New("server version: no rows")
	}

	raw := string(results[0].Rows[0][0])
	version, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.Wrapf(err, "server version %q", raw)
	}
	return version, nil
}

// replicationPluginArguments builds the pgoutput options for START_REPLICATION.
// Logical decoding messages (pg_logical_emit_message) are sent only when
// messages is requested. That option is independent of streaming and exists
// on proto version 1, but PostgreSQL 13 and older reject it.
func replicationPluginArguments(protoVersion, serverVersionNum int) []string {
	args := []string{
		fmt.Sprintf("proto_version '%d'", protoVersion),
	}
	if serverVersionNum >= logicalMessagesMinServerVersion {
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
