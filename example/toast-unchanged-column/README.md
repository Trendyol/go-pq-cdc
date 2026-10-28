# Unchanged TOAST column semantics on a key-changing UPDATE

Demonstrates the fixed behaviour in `pq/message/format/update.go`. An unchanged TOASTed (`'u'`)
column of an UPDATE is never reported as NULL. It is either carried through from the old tuple
or, when the old tuple has no real value for it, omitted from `NewDecoded` and listed by name in
`UnchangedToastColumns`.

The old tuple's values are only used when they are real: a full old row (`'O'`, REPLICA IDENTITY
FULL) or a key column. The `'n'` placeholders of a `'K'` (key only) old tuple are never copied
into non-key columns.

With `REPLICA IDENTITY DEFAULT` and an UPDATE that changes the primary key, the old tuple is `'K'`,
so an unchanged TOASTed `payload` is listed in `UnchangedToastColumns`, not reported as `nil`.

When no old tuple is sent (primary key not changed), `payload` is also listed in
`UnchangedToastColumns`.

## Run

```sh
docker compose up -d
go run .            # from this directory; leave it running
```

The connector listens on `public.docs` (`REPLICA IDENTITY DEFAULT`). Each UPDATE prints
one block. Run the following in a second terminal:

```sh
psql "postgres://cdc_user:cdc_pass@127.0.0.1:5433/cdc_db"
```

### A) Non-key UPDATE: expect NOTE (payload marked unchanged)

```sql
UPDATE docs SET title = 'b' WHERE id = 1;
```

```
---- UPDATE public.docs
  OldTupleType:        N
  NewDecoded[payload]: <absent>
  OldDecoded[payload]: <absent>
  UnchangedToastColumns: [payload]
  NOTE: payload marked unchanged, no old tuple sent so its value is not in this message
```

### B) Primary-key-changing UPDATE: expect OK (payload marked unchanged)

```sql
UPDATE docs SET id = 2 WHERE id = 1;
```

```
---- UPDATE public.docs
  OldTupleType:        K
  NewDecoded[payload]: <absent>
  OldDecoded[payload]: nil
  UnchangedToastColumns: [payload]
  OK: payload marked unchanged
```

### C) Primary-key-changing UPDATE with REPLICA IDENTITY FULL: expect OK (payload carried)

```sql
ALTER TABLE docs REPLICA IDENTITY FULL; UPDATE docs SET id = 3 WHERE id = 2;
```

```
---- UPDATE public.docs
  OldTupleType:        O
  NewDecoded[payload]: "xxxxxxxxxxxxxxxxxxxx..." (len 5000)
  OldDecoded[payload]: "xxxxxxxxxxxxxxxxxxxx..." (len 5000)
  UnchangedToastColumns: []
  OK: payload carried through
```

Run the steps in order. Step C depends on the rows created by A and B.

Afterwards, restore the table with `ALTER TABLE docs REPLICA IDENTITY DEFAULT;` before the
peek section below, which runs with the connector stopped and assumes `DEFAULT`. (A connector
restart would reset it to `DEFAULT` by itself via the publication config.)

## Peek at the raw pgoutput bytes

```sql
SELECT encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('cdc_slot', NULL, NULL, 'proto_version','1', 'publication_names','cdc_publication');
```

This only works while the connector is **not** attached to the slot. Stop `go run .`
first. `peek` does not consume changes, so the connector can read them later.

For `UPDATE docs SET id = 4 WHERE id = 3` (`REPLICA IDENTITY DEFAULT`, `title` is `'b'`),
the `U` row observed was:

```
55 00004001 4b 0003 74 00000001 33  6e  6e   4e 0003 74 00000001 34  74 00000001 62  75
U  rel oid  K  3col t  len=1    '3' n   n    N  3col t  len=1    '4' t  len=1    'b' u
```

`4b` ('K') is the old tuple type, the two `6e` ('n') bytes are the NULL placeholders for the
non-key `title` and `payload` columns, and the trailing `75` ('u') is the unchanged TOASTed
`payload` in the new tuple. The decoder does not copy the `n` placeholders into the `u` slot,
so `payload` is listed in `UnchangedToastColumns`.

## Cleanup

```sh
docker compose down -v
```
