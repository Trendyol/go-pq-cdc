package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/3n0ugh/dcpg"
	"github.com/3n0ugh/dcpg/config"
	"github.com/3n0ugh/dcpg/pq"
	"github.com/3n0ugh/dcpg/pq/message/format"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log/slog"
	"math"
	"os"
	"runtime"
	"strconv"
	"time"
)

/*
	psql "postgres://dcp_user:dcp_pass@127.0.0.1/dcp_db?replication=database"

	CREATE TABLE users (
	 id serial PRIMARY KEY,
	 name text NOT NULL,
	 created_on timestamptz
	);

	INSERT INTO users (name)
	SELECT
		'Oyleli' || i
	FROM generate_series(1, 100) AS i;
*/

type Message struct {
	Message esutil.BulkIndexerItem
	Ack     func() error
}

func main() {
	ctx := context.Background()

	esCfg := elasticsearch.Config{
		MaxRetries:            math.MaxInt,
		Addresses:             []string{"http://localhost:9200"},
		CompressRequestBody:   false,
		DiscoverNodesOnStart:  true,
		DiscoverNodesInterval: 5 * time.Minute,
	}

	w, err := NewElasticsearchBulkIndexer(esCfg, "dcpg_index")
	if err != nil {
		slog.Error("new elasticsearch bulk indexer", "error", err)
	}

	defer func() {
		err = w.Close(ctx)
		if err != nil {
			slog.Error("elasticsearch bulk indexer close", "error", err)
		}
	}()

	messages := make(chan Message, 10000)
	go Produce(ctx, w, messages)

	cfg := config.Config{
		Host:     "127.0.0.1",
		Username: "dcp_user",
		Password: "dcp_pass",
		Database: "dcp_db",
		Publication: config.PublicationConfig{
			Name:         "dcp_publication",
			Create:       true,
			DropIfExists: true,
		},
		Slot: config.SlotConfig{
			Name:   "dcp_slot",
			Create: true,
		},
	}

	connector, err := dcpg.NewConnector(ctx, cfg, FilteredMapper(messages))
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	connector.Start(ctx)
}

func NewElasticsearchBulkIndexer(cfg elasticsearch.Config, indexName string) (esutil.BulkIndexer, error) {
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	res, err := esClient.Indices.Create(indexName)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:         indexName,
		Client:        esClient,
		NumWorkers:    runtime.NumCPU(),
		FlushBytes:    int(5e+6),
		FlushInterval: 100 * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}

	return bi, nil
}

func FilteredMapper(messages chan Message) pq.ListenerFunc {
	return func(ctx pq.ListenerContext) {
		switch msg := ctx.Message.(type) {
		case *format.Insert:
			encoded, _ := json.Marshal(msg.Decoded)
			messages <- Message{
				Message: esutil.BulkIndexerItem{
					Action:     "index",
					DocumentID: strconv.Itoa(int(msg.Decoded["id"].(int32))),
					Body:       bytes.NewReader(encoded),
					OnSuccess: func(_ context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						slog.Info("es insert doc success", "id", item.DocumentID)
						if err := ctx.Ack(); err != nil {
							slog.Error("ack", "error", err)
						}
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							slog.Error("elasticsearch create document", "error", err)
						} else {
							slog.Error("elasticsearch create document", "type", res.Error.Type, "error", err)
						}
					},
				},
				Ack: ctx.Ack,
			}
		case *format.Delete:
			messages <- Message{
				Message: esutil.BulkIndexerItem{
					Action:     "delete",
					DocumentID: strconv.Itoa(int(msg.OldDecoded["id"].(int32))),
					OnSuccess: func(_ context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						slog.Info("es delete doc success", "id", item.DocumentID)
						if err := ctx.Ack(); err != nil {
							slog.Error("ack", "error", err)
						}
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							slog.Error("elasticsearch delete document", "error", err)
						} else {
							slog.Error("elasticsearch delete document", "type", res.Error.Type, "error", err)
						}
					},
				},
				Ack: ctx.Ack,
			}
		case *format.Update:
			encoded, _ := json.Marshal(msg.NewDecoded)
			messages <- Message{
				Message: esutil.BulkIndexerItem{
					Action:     "update",
					DocumentID: strconv.Itoa(int(msg.NewDecoded["id"].(int32))),
					Body:       bytes.NewReader(encoded),
					OnSuccess: func(_ context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						slog.Info("es update doc success", "id", item.DocumentID)
						if err := ctx.Ack(); err != nil {
							slog.Error("ack", "error", err)
						}
					}, OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							slog.Error("elasticsearch update document", "error", err)
						} else {
							slog.Error("elasticsearch update document", "type", res.Error.Type, "error", err)
						}
					},
				},
				Ack: ctx.Ack,
			}
		}
	}
}

func Produce(ctx context.Context, w esutil.BulkIndexer, messages <-chan Message) {
	var err error
	for {
		event := <-messages
		err = w.Add(ctx, event.Message)
		if err != nil {
			slog.Error("elasticsearch bulk indexer item add", "error", err)
		}
	}
}
