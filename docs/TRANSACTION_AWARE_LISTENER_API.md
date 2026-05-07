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

	// AckLSN is the exact LSN that Ack will confirm.
	// For commit events this is the transaction end LSN.
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
- row messages: `*format.Insert`, `*format.Update`, `*format.Delete`
- `*format.Commit`
- buffered streamed transaction rows followed by `*format.StreamCommit`
- snapshot events as they work today

## Semantics

For regular transactions:

1. Emit `Begin`.
2. Emit row events in WAL/order-preserving order.
3. Emit `Commit`.
4. `Commit.Ack()` confirms the transaction end LSN.

For streamed transactions:

1. Do not emit rows before the streamed transaction commits.
2. Buffer streamed rows by XID as today.
3. On `StreamCommit`, emit the buffered rows in order.
4. Emit `StreamCommit`.
5. `StreamCommit.Ack()` confirms the transaction end LSN.
6. On `StreamAbort`, discard buffered rows and emit nothing.

For rollbacks:

- no committed row events are emitted
- no commit boundary is emitted

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
		if err := projector.Apply(txBuffer.Rows(), msg.TransactionEndLSN); err != nil {
			return
		}
		_ = ctx.Ack()
	}
}
```

The important property is that replication is acknowledged only after the consumer has durably applied the full source transaction.

## Test Coverage

Covered behavior:

- default mode does not emit transaction boundary messages
- transaction mode emits `Begin -> rows -> Commit`
- `Commit` context has `AckLSN == TransactionEndLSN`
- multi-row transactions preserve row order
- streamed transactions emit no rows before `StreamCommit`
- `StreamAbort` discards buffered rows
- listener can delay `Ack()` until after commit projection
- sink shutdown closes/stops processor cleanly without blocking

## Compatibility

This is introduced as an opt-in feature. Existing users that only handle row messages keep the current default behavior. Transactional consumers get a stable API instead of relying on internal buffering details.
