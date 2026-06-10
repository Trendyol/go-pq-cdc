# Transaction-Aware Listener Metadata API

## Summary

`go-pq-cdc` works well for row-oriented consumers, but some consumers need a stronger listener contract: they need to project a full committed PostgreSQL transaction atomically, persist the commit/checkpoint LSN, and only acknowledge replication after their projection is durable.

This adds an opt-in transaction-aware listener mode that exposes WAL metadata and transaction boundary events to the listener.

The default row-oriented behavior remains unchanged for existing users.

## Problem

The current listener callback receives decoded messages and an `Ack` function. For transactional projectors, that is not enough.

A consumer that maintains a derived read model or sync log often needs to:

- group all row changes from one PostgreSQL transaction
- apply those changes atomically to its own tables
- store the transaction commit LSN as an idempotency/checkpoint key
- call `Ack()` only after the whole projected transaction commits
- avoid acknowledging partial transaction state

Without explicit transaction boundary events, the consumer cannot reliably know when to flush an accumulated transaction. Without exposed WAL metadata, it also cannot persist the exact source position associated with a row or commit.

This is generic CDC behavior, not application-specific mapping logic.

## API

Add WAL metadata to listener contexts:

```go
type ListenerContext struct {
	Message any
	Ack     func() error

	// WALStart is the WAL start position for this decoded logical message.
	WALStart pq.LSN

	// AckLSN is the LSN associated with this message's acknowledgement.
	// In transaction-aware mode, only Commit and StreamCommit acknowledgements
	// advance confirmed_flush_lsn; non-commit acknowledgements are non-advancing.
	AckLSN pq.LSN
}
```

Add an opt-in config flag:

```go
type ListenerConfig struct {
	EmitTransactionBoundaries bool `json:"emitTransactionBoundaries" yaml:"emitTransactionBoundaries"`
}
```

When `EmitTransactionBoundaries` is false, behavior remains row-oriented.

When `EmitTransactionBoundaries` is true, the listener receives:

- `*format.Begin`
- decoded row and metadata messages in WAL order, such as `*format.Insert`, `*format.Update`, `*format.Delete`, `*format.Relation`, and `*format.Truncate` when PostgreSQL emits them
- `*format.Commit`
- buffered streamed transaction messages followed by `*format.StreamCommit`
- snapshot events as they work today

## Semantics

For regular transactions:

1. Emit `Begin`.
2. Emit decoded row and metadata events in WAL/order-preserving order.
3. Emit `Commit`.
4. `Ack()` on `Commit` is the transaction checkpoint acknowledgement.

For streamed transactions:

1. Do not emit streamed transaction messages before the streamed transaction commits.
2. Buffer streamed messages by XID as today.
3. On `StreamCommit`, emit the buffered messages in order.
4. Emit `StreamCommit`.
5. `Ack()` on `StreamCommit` is the transaction checkpoint acknowledgement.
6. On `StreamAbort`, discard buffered messages and emit nothing.

For rollbacks:

- no committed row events are emitted
- no commit boundary is emitted

For acknowledgements in transaction-aware mode:

- `Ack()` on `Commit` and `StreamCommit` is the only operation that may advance `confirmed_flush_lsn`.
- `Ack()` on non-commit messages sends a standby status update with the current confirmed LSN, but does not advance `confirmed_flush_lsn`, even when the message's `AckLSN` is a transaction-end LSN.
- Commit acknowledgements are ordered. A later commit acknowledgement is held behind any earlier unacknowledged commit, so a later `Ack()` cannot accidentally confirm an unprojected earlier transaction.
- `Ack()` is still cumulative at the PostgreSQL replication-slot level. Transaction-aware mode adds an ordered checkpoint guard around commit acknowledgements; consumers must still retry, block, stop the connector, or terminate the process after a projection failure instead of continuing to process later commits.

Heartbeat rows are still filtered before delivery. In transaction-aware mode, heartbeat row auto-acks are non-advancing. Boundary messages around heartbeat transactions may still be visible; consumers should acknowledge every visible commit boundary or disable heartbeat for that listener mode.

## Why This Matters

This enables downstream systems to implement correct transactional projection:

```go
func listener(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Begin:
		txBuffer.Begin(msg.Xid)

	case *format.Insert, *format.Update, *format.Delete:
		txBuffer.Append(ctx.WALStart, msg)

	case *format.Commit:
		if err := projector.Apply(txBuffer.Rows(), ctx.AckLSN); err != nil {
			// Do not continue to later commits after a failed projection.
			// Retry, block, cancel the connector, or terminate the process.
			failHard(err)
			return
		}
		if err := ctx.Ack(); err != nil {
			failHard(err)
		}
	}
}
```

The important property is that replication is acknowledged only after the consumer has durably applied the full source transaction.

## Test Coverage

Covered behavior:

- default mode does not emit transaction boundary messages
- default mode keeps immediate row-oriented acknowledgement behavior
- transaction mode emits `Begin -> rows -> Commit`
- `Commit` context has `AckLSN == TransactionEndLSN`
- non-commit `Ack()` does not advance `confirmed_flush_lsn` in transaction-aware mode
- later commit acknowledgements cannot advance past an earlier unacknowledged commit in transaction-aware mode
- multi-row transactions preserve row order
- streamed transactions emit no rows before `StreamCommit`
- `StreamAbort` discards buffered rows
- listener can delay `Ack()` until after commit projection
- sink shutdown closes/stops processor cleanly without blocking

## Compatibility

This is introduced as an opt-in feature. Existing users that only handle row messages keep the current default behavior. Transactional consumers get a stable API instead of relying on internal buffering details.
