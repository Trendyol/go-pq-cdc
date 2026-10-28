# Unchanged TOAST column reported as NULL on a key-changing UPDATE

Reproduces a suspected bug in `pq/message/format/update.go`: when an old tuple is
present, the same-index old column is copied into every `'u'` (unchanged TOAST)
column of the new tuple, without checking whether the old tuple is `'K'` (key only,
non-key columns are `'n'` NULL placeholders) or `'O'` (full old row).

With `REPLICA IDENTITY DEFAULT` and an UPDATE that changes the primary key, the old
tuple is `'K'`, so an unchanged TOASTed `payload` shows up as an explicit `nil` in
`NewDecoded`, indistinguishable from a real `SET payload = NULL`.

When no old tuple is sent (primary key not changed), `pq/message/tuple/data.go` has no
`DataTypeToast` case, so the column is silently absent from `NewDecoded`.

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

### A) Non-key UPDATE: expect NOTE (payload absent)

```sql
UPDATE docs SET title = 'b' WHERE id = 1;
```

```
---- UPDATE public.docs
  OldTupleType:        N
  NewDecoded[payload]: <absent>
  OldDecoded[payload]: <absent>
  NOTE: payload silently absent from NewDecoded (unchanged TOAST, no old tuple)
```

### B) Primary-key-changing UPDATE: expect BUG (payload nil)

```sql
UPDATE docs SET id = 2 WHERE id = 1;
```

```
---- UPDATE public.docs
  OldTupleType:        K
  NewDecoded[payload]: nil
  OldDecoded[payload]: nil
  BUG: payload reported as NULL although the UPDATE never touched it (unchanged TOAST copied from 'K' tuple)
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
`payload` in the new tuple. The decoder copies the `n` from the `K` tuple into that `u` slot.

## Cleanup

```sh
docker compose down -v
```
