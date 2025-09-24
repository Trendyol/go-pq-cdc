package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Configuration ---
const DATABASE_URL = "postgres://go_cdc_user:a_very_strong_password@localhost:5432/promotion_report"

const (
	NUM_WORKERS       = 4
	BATCH_SIZE        = 2
	EMPTY_FETCH_LIMIT = 5
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, DATABASE_URL)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}
	defer pool.Close()
	log.Println("Successfully connected to PostgreSQL.")

	log.Println("Coordinator: Starting transaction and exporting snapshot...")
	coordinatorConn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatalf("Coordinator: Failed to acquire connection: %v\n", err)
	}

	tx, err := coordinatorConn.Begin(ctx)
	if err != nil {
		log.Fatalf("Coordinator: Could not begin transaction: %v\n", err)
	}
	defer func() {
		log.Println("Coordinator: Closing transaction.")
		tx.Rollback(ctx)
		coordinatorConn.Release()
	}()

	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		log.Fatalf("Coordinator: Could not set isolation level: %v\n", err)
	}

	var snapshotID string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID); err != nil {
		log.Fatalf("Coordinator: Could not export snapshot: %v\n", err)
	}
	log.Printf("Coordinator: Snapshot exported with ID: %s. Dispatching workers...\n", snapshotID)

	rows, err := tx.Query(ctx, `
SELECT
    column_name,
table_name,
table_schema
FROM
    information_schema.columns
WHERE
    table_schema = 'public'  -- Specify the schema (usually 'public')
    AND table_name = 'meal_orders_metric'   -- Specify your table name
ORDER BY
    ordinal_position;`)
	if err != nil {
		log.Fatalf("Coordinator: Could not query rows: %v\n", err)
	}

	tableFields := make([]string, 0)
	for rows.Next() {
		var tf string
		err = rows.Scan(&tf)
		if err != nil {
			log.Fatalf("Coordinator: Could not scan row: %v\n", err)
		}
		tableFields = append(tableFields, tf)
	}

	// --- DYNAMIC DISPATCH STATE ---
	// Atomic counters for coordinating workers without pre-calculated ranges.
	var chunkIndex uint64 = 0
	var consecutiveEmptyFetches uint64 = 0

	var wg sync.WaitGroup
	errs := make(chan error, NUM_WORKERS)

	for i := 0; i < NUM_WORKERS; i++ {
		wg.Add(1)
		// Workers no longer get a start/end ID. They get pointers to the shared atomic state.
		go runWorker(
			ctx,
			pool,
			i+1,
			snapshotID,
			&chunkIndex,
			&consecutiveEmptyFetches,
			&wg,
			errs,
			tableFields,
		)
	}

	log.Println("Coordinator: Waiting for all workers to complete...")
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			log.Fatalf("A worker failed: %v", err)
		}
	}

	log.Println("All workers completed successfully!")
}

// runWorker now pulls chunk offsets dynamically from an atomic counter.
func runWorker(ctx context.Context, pool *pgxpool.Pool, workerID int, snapshotID string, chunkIndex, emptyFetches *uint64, wg *sync.WaitGroup, errs chan<- error, tf []string) {
	defer wg.Done()
	log.Printf("[Worker %d] Starting.\n", workerID)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		errs <- fmt.Errorf("worker %d: failed to acquire connection: %w", workerID, err)
		return
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	})
	if err != nil {
		errs <- fmt.Errorf("worker %d: could not begin transaction: %w", workerID, err)
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID)); err != nil {
		errs <- fmt.Errorf("worker %d: could not set snapshot: %w", workerID, err)
		return
	}

	// This is the worker's main loop.
	for {
		// --- Termination Check ---
		if atomic.LoadUint64(emptyFetches) >= EMPTY_FETCH_LIMIT {
			log.Printf("[Worker %d] Termination condition met. Stopping.\n", workerID)
			break
		}

		// --- Claim a Chunk ---
		myChunkIndex := atomic.AddUint64(chunkIndex, 1) - 1
		myOffset := myChunkIndex * BATCH_SIZE

		query := `
			SELECT *
			FROM meal_orders_metric
			ORDER BY CTID
			LIMIT $1 OFFSET $2`

		rows, err := tx.Query(ctx, query, BATCH_SIZE, myOffset)
		if err != nil {
			errs <- fmt.Errorf("worker %d: query failed with offset %d: %w", workerID, myOffset, err)
			return
		}

		// --- Process Batch ---
		var itemsProcessedInBatch int
		for rows.Next() {
			itemsProcessedInBatch++

			v, err := rows.Values()
			if err != nil {
				fmt.Printf("Worker %d: could not scan row %d: %v\n", workerID, myOffset, err)
			}

			v2 := map[any]any{}

			for i, vv := range v {
				v2[tf[i]] = vv
			}

			// V V V THIS IS THE LINE THAT PRINTS THE READ VALUES V V V
			fmt.Printf("[Worker %d] Processed Row: %v\n", workerID, v2)
		}
		rows.Close()

		// --- Update Termination State ---
		if itemsProcessedInBatch > 0 {
			atomic.StoreUint64(emptyFetches, 0)
		} else {
			atomic.AddUint64(emptyFetches, 1)
			log.Printf("[Worker %d] Fetched an empty batch at offset %d. Consecutive empty count is now ~%d.\n", workerID, myOffset, atomic.LoadUint64(emptyFetches))
		}
	}

	if err := tx.Commit(ctx); err != nil {
		errs <- fmt.Errorf("worker %d: could not commit transaction: %w", workerID, err)
		return
	}
}
