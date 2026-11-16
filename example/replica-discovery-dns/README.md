# Replica discovery from DNS

`visibilityGuard.discovery.dns` makes the connector hold every transaction until every standby behind a set of DNS
names has replayed its commit. This lab runs it against real standbys and a DNS server whose records you edit from the
host while the connector runs, so you can watch the gate follow the records. See the
[main README](../../README.md#replica-discovery-dns) for the contract.

## Topology

`docker-compose.yml` starts a primary, two standbys, a CoreDNS and the connector. Everything has a fixed address.

| service     | address                       | what it is |
|-------------|-------------------------------|------------|
| `primary`   | `10.99.0.10` (host `5447`)    | PostgreSQL 17, logical replication |
| `mars`      | `10.99.0.11`, `10.99.1.11` (host `5448`) | standby, caught up. Two addresses, see step 3 |
| `venus`     | `10.99.0.12` (host `5449`)    | standby with `recovery_min_apply_delay = 5s`: every commit is applied 5 seconds late |
| `dns`       | `10.99.0.53` (host `5354`, TCP and UDP) | CoreDNS, records in `dns/hosts` |
| `connector` | `10.99.0.20`                  | `main.go`, built from this working tree (`replace ... => ../..`) |

`dns/hosts` is the only source of records:

```
10.99.0.11 replica.demo.mars.lab
10.99.0.12 replica.demo.venus.lab
10.99.1.11 replica.demo.venus.lab
```

Two names, one source each: `replica.demo.mars.lab` is mars, `replica.demo.venus.lab` is venus plus a spare address,
`10.99.1.11`, which is mars's second address. The spare exists for step 3: two names may not list the same `host:port`,
and a name may not become empty, so a name that must lose venus needs another real standby behind it. CoreDNS reloads
the file every second. A name with no line answers NXDOMAIN (an empty answer); a known name asked for AAAA answers an
empty NOERROR.

The connector config is `config/connector-both.yml` (both names) or `config/connector-mars.yml` (only the mars name):
`refreshInterval: 5s`, `failMode: closed`, `nameserver: 10.99.0.53:53` (queried over TCP), `timeout: 25s`. For every
insert event the handler in `main.go` logs how long after the insert the event arrived (`held_ms`) and whether the
row is readable right now on mars and on venus:

```
INFO event id=2 held_ms=5086 mars=true venus=true
```

A standby the connector gates is always `true`. One it does not gate may be `false`: that is the incident this
feature prevents.

## Prerequisites

Docker with compose, and `dig` (preinstalled on macOS). Go is not needed on the host: the connector is built in
Docker from the repository root. The first run builds the image and pulls the PostgreSQL and CoreDNS images.

## Quick start

```bash
cd example/replica-discovery-dns
./lab.sh            # all five steps, then docker compose down -v
./lab.sh 3          # one step; the lab stays up
./lab.sh down       # remove everything
```

Every step prints PASS or FAIL lines, and the script exits 1 on any FAIL.

## The steps

1. **Only the mars name configured.** An insert arrives at once (`held_ms` a few ms) with `mars=true venus=false`.
   Venus is not a gated member, so the event outruns its apply delay: a read from venus misses the row.
2. **Both names configured.** The event is held about 5 seconds, the apply delay, and arrives with `mars=true
   venus=true`. The guard polled every address behind both names before dispatching.
3. **Drop venus without a restart.** The line `10.99.0.12 replica.demo.venus.lab` is removed; the venus name still
   resolves to `10.99.1.11`, so the refresh succeeds. After one refresh interval the next transaction re-reads DNS, the
   connector logs `replica guard replica set changed` with `"removed":["10.99.0.12:5432"]`, and the event arrives at once
   with `venus=false`. The stream is not restarted (`cdc stream started` appears once). This is the layout's way to
   show a member leaving: venus leaves its name while the spare keeps the name non-empty.
4. **Put it back.** The line returns; after a refresh the log says `"added":["10.99.0.12:5432"]` and the event is
   held about 5 seconds again with `venus=true`.
5. **Empty a name.** Both venus lines are removed, so the name answers NXDOMAIN. The connector logs
   `replica guard refresh failed ... no A or AAAA record; an empty answer is a source failure, not zero replicas` and the
   insert is not dispatched (the stream holds, it does not fall back to "no replicas"). Restoring the records releases
   the held event within seconds. The connector's `timeout` is 25s: if the name stays empty longer, `failMode: closed`
   stops the stream and the event is redelivered after a restart. The connector service has no restart policy, so after
   the gate timeout the container stays exited until `docker compose up -d connector`.

## Playing by hand

```bash
docker compose up -d --build --wait
docker compose logs -f --no-log-prefix connector       # the connector and every event line
docker compose exec primary psql -U cdc_user -d cdc_db -c "INSERT INTO orders (note) VALUES ('x')"
```

**Edit the DNS records.** Edit `dns/hosts` (one `ip name` line per record) in place and save; CoreDNS picks it up within
a second. Check what the lab DNS answers, from the host, over TCP (the connector's `nameserver` option always uses TCP)
and over UDP:

```bash
dig @127.0.0.1 -p 5354 +tcp +short replica.demo.venus.lab A
dig @127.0.0.1 -p 5354 +tcp replica.demo.venus.lab AAAA     # NOERROR, no answer
dig @127.0.0.1 -p 5354 +short replica.demo.mars.lab A       # UDP
dig @127.0.0.1 -p 5354 +tcp replica.demo.nobody.lab A       # NXDOMAIN
```

The connector only re-reads DNS when a transaction is waiting and its member list is older than `refreshInterval` (5s):
after an edit, wait that long, then insert.

**Switch the configured names.** The service reads `config/connector-$CONFIG`; recreate it to switch:

```bash
CONFIG=connector-mars.yml docker compose up -d --force-recreate --no-deps connector   # only the mars name
CONFIG=connector-both.yml docker compose up -d --force-recreate --no-deps connector   # both names (default)
```

To change the names themselves, edit `names:` in either file and recreate the same way.

**Log lines to watch** (`docker compose logs -f --no-log-prefix connector | grep -E 'replica guard|dnsdiscovery|event'`):

- `replica guard replica set changed`: the resolved set grouped by source (one source per DNS name), with `added` and
  `removed`. Logged at start and on every change.
- `replica guard dropped a replica the source no longer lists`: a member that was failing and then left the answer.
- `replica guard refresh failed, retrying until the gate deadline`: a name failed (NXDOMAIN, no record, DNS down).
- `replica guard slow wait completed`: how long a transaction was held and the replay position of every replica.
- `dnsdiscovery: no nameserver is set ...` (Warn at startup): remove the `nameserver:` line from a config copy and start
  with it. The connector then uses the container's system resolver, which does not know `.lab`, so it also fails to
  start with `no A or AAAA record`: the lab DNS is only reachable through the `nameserver` option.
- `event id=... held_ms=... mars=... venus=...`: the handler, from `main.go`.

**Cleanup.**

```bash
docker compose down -v
```

## Limits

- A lab DNS is not Consul. CoreDNS serves the file as is: there are no health checks, so a stopped standby stays in the
  answer until you edit the file (try `docker compose stop venus` and watch the gate hold, then fail by timeout).
- The 3-record UDP cut Consul applies to answers without EDNS (`udp_answer_limit`) does not exist here, so the
  `nameserver` TCP path is exercised but the failure it protects against is not reproduced.
- The spare address in the venus name is a lab device for step 3, not a pattern: in production a name lists the
  standbys the registry reports as healthy.
- Timelines are unknown to DNS, so members are polled without the timeline rule.
