package metadata

import (
	"context"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/jackc/pgx/v5"
)

type Chunks []Chunk
type Chunk struct {
	ID          int    `json:"id"`
	Publication string `json:"publication"`
	Slot        string `json:"slot"`
	TableName   string `json:"table_name"`
	Schema      string `json:"schema"`
	SnapshotID  string `json:"snapshot_id"`
	Limit       int    `json:"limit"`
	Offset      int    `json:"offset"`
}

func NewChunks(ctx context.Context, cfg config.Config, snapshot string, tableCounts map[string]int) Chunks {
	return Chunks{}
}

func (m *MetaData) CreateChunkTable(ctx context.Context) error {
	return nil
}

func (m *MetaData) SaveChunks(ctx context.Context, chunks Chunks) error {
	return nil
}

func (m *MetaData) GetAvailableChunk(ctx context.Context, tx pgx.Tx) (*Chunk, error) {
	return nil, nil
}

func (m *MetaData) DeleteChunk(ctx context.Context, tx pgx.Tx, id int) error {
	return nil
}
