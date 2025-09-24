package metadata

import (
	"context"
	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/jackc/pgx/v5"
)

type Checkpoint struct {
	PublicationName string `json:"publication_name"`
	SlotName        string `json:"slot_name"`

	InitialSnapshot      string `json:"initial_snapshot"`
	InitialStrategy      int    `json:"initial_strategy"`
	InitialDone          bool   `json:"initial_done"`
	InitialChunksCreated bool   `json:"initial_chunks_created"`
}

func NewCheckpoint(cfg config.Config) *Checkpoint {
	return &Checkpoint{
		PublicationName:      cfg.Publication.Name,
		SlotName:             cfg.Slot.Name,
		InitialSnapshot:      "",
		InitialStrategy:      cfg.Initial.Strategy,
		InitialDone:          false,
		InitialChunksCreated: false,
	}
}

func (c *Checkpoint) IsRequireChunks() bool {
	return true
}

//	Lock the table and check the checkpoint is existing;
//	If not create the checkpoint, then check the initial strategy is 1 and initial done is false and initial_chunks_created is false;
//		If yes, create the chunks rows by publication tables and set initial chunks created true;
//		If no, release the lock;
//	If exists, check the initial strategy is 1 and initial done is true or initial chunks created is true;
//		If yes, release the lock;
//		If no, create the chunks rows by publication tables;

func (m *MetaData) CreateCheckpointTable(ctx context.Context) error {
	return nil
}

func (m *MetaData) SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	return nil
}

func (m *MetaData) UpdateChunksCreated(ctx context.Context) error {
	return nil
}

func (m *MetaData) UpdateInitialDone(ctx context.Context, tx pgx.Tx) error {
	return nil
}
