### TODOs
- [ ] Add README.md
- [ ] Add leader election (take the responsibility when slot is not active)
- [ ] Add integration test cases documentation
  - [x] Write possible cases and make priority list for implementation
  - [x] Add priority list to todo.md
  - [x] Add Basic Functionality Test
  - [ ] Add Copy Protocol Test
  - [ ] Add Several Transactions Test
  - [ ] Add Multi Value Insert Test
  - [ ] Add Concurrent Operations Test
  - [x] Add Transaction Rollback Commit Test
  - [ ] Add System IDENTIFY FULL Test
- [ ] Add unit tests
- [ ] Add Contribution.md
- [ ] Add ci pipeline for project
  - [ ] Go Lint 
  - [ ] Go Vet
  - [ ] Go vulncheck
  - [ ] Go Test (-race)
  - [ ] Sonar - or any other code analysis tool
- [ ] Add cd pipeline for project
  - [ ] Create Config&Run docker images
  - [ ] Add goreleaser implementation
- [ ] Add License file
- [ ] Add Dockerfile
- [ ] Create Grafana dashboard
  - [ ] Expose pg_stats

https://stormatics.tech/blogs/logical-replication-in-postgresql

https://www.postgresql.org/docs/16/monitoring.html
pg_stat_subscription_stats
```sql
SELECT *,
       Pg_current_wal_lsn(),
       Pg_size_pretty(Pg_wal_lsn_diff(Pg_current_wal_lsn(), restart_lsn))AS retained_walsize,
       Pg_size_pretty(Pg_wal_lsn_diff(Pg_current_wal_lsn(), confirmed_flush_lsn)) AS subscriber_lag
FROM pg_replication_slots;  
```

```sql
select pid,
       application_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) sending_lag,
       pg_size_pretty(pg_wal_lsn_diff(sent_lsn, flush_lsn)) receiving_lag,
       pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) replaying_lag,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) total_lag
from pg_stat_replication;
```
