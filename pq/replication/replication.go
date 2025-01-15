package replication

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/vskurikhin/go-pq-cdc/pq"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Replication struct {
	conn pq.Connection
}

func New(conn pq.Connection) *Replication {
	return &Replication{conn: conn}
}

func (r *Replication) Start(publicationName, slotName string) error {
	startLSN := pq.LSN(2)

	pluginArguments := append([]string{
		"proto_version '2'",
		"messages 'true'",
		"streaming 'true'",
	}, "publication_names '"+publicationName+"'")

	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s (%s)", slotName, startLSN, strings.Join(pluginArguments, ","))
	r.conn.Frontend().SendQuery(&pgproto3.Query{String: sql})
	err := r.conn.Frontend().Flush()
	if err != nil {
		return errors.Wrap(err, "start replication")
	}
	return nil
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
