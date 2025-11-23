# Go-PQ-CDC-Kafka Scaling Guide

This guide explains the necessary steps to run multiple instances of the `go-pq-cdc-kafka` service.

## Changes Made

### 1. Docker Compose Changes
- ✅ `container_name` removed (Docker Compose will automatically name each instance)
- ✅ Port mapping replaced with `expose` (to prevent port conflicts)
- ✅ Metrics are only exposed on the internal network

### 2. Prometheus Configuration
- ✅ DNS service discovery added
- ✅ All scaled instances will be automatically scraped

## Usage

### Starting Multiple Instances

**To start with 3 instances:**
```bash
cd benchmark/benchmark_initial
docker-compose up --scale go-pq-cdc-kafka=3 -d
```

**To start with 5 instances:**
```bash
docker-compose up --scale go-pq-cdc-kafka=5 -d
```

### Viewing Running Instances

```bash
docker-compose ps go-pq-cdc-kafka
```

Or:
```bash
docker ps | grep go-pq-cdc-kafka
```

### Changing the Number of Instances

**Scaling while running (e.g., from 3 to 5):**
```bash
docker-compose up --scale go-pq-cdc-kafka=5 -d
```

**Scale down (e.g., from 5 to 2):**
```bash
docker-compose up --scale go-pq-cdc-kafka=2 -d
```

### Monitoring Logs of a Specific Instance

```bash
# Logs of all instances
docker-compose logs -f go-pq-cdc-kafka

# Specific container
docker logs -f benchmark_initial_go-pq-cdc-kafka_1
docker logs -f benchmark_initial_go-pq-cdc-kafka_2
```

### Checking Instances in Prometheus

In Prometheus UI (http://localhost:9090):
1. Go to Status → Targets
2. Find the `go_pq_cdc_exporter` job
3. You will see all scaled instances listed

## Important Notes

### ⚠️ Things to Be Careful About

1. **Snapshot Mode**: Currently, each instance is running in `SnapshotModeSnapshotOnly` mode. This means each instance will try to take a snapshot from the database. Be careful with coordination.

2. **Replication Slot**: Each instance cannot use the same PostgreSQL replication slot. If you're using replication, different slot names are required for each instance.

3. **Kafka Partitions**: If you're using multiple instances, it's important for performance that your Kafka topic has multiple partitions.

4. **Resource Limits**: CPU and memory limits for each instance:
   - CPU Limit: 1 core
   - Memory Limit: 512MB
   - 3 instances = total 3 cores, 1.5GB RAM

### 📊 Monitoring

In Grafana (http://localhost:3000) you can see metrics for all instances:
- CPU usage
- Memory usage
- Kafka produce rate
- CDC lag

### 🔧 Troubleshooting

**Problem: Instances are not starting**
```bash
# Check logs
docker-compose logs go-pq-cdc-kafka

# Check health status
docker-compose ps
```

**Problem: Prometheus is not seeing instances**
```bash
# Test DNS resolution
docker-compose exec prometheus nslookup tasks.go-pq-cdc-kafka
```

**Problem: Insufficient resources**
```bash
# Check resource usage
docker stats
```

## Alternative: Manual Instance Definition

If you want different configuration for each instance, you can define them manually in docker-compose.yml:

```yaml
  go-pq-cdc-kafka-1:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... other settings

  go-pq-cdc-kafka-2:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... other settings

  go-pq-cdc-kafka-3:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... other settings
```

## Performance Tips

1. **Batch Size**: There's a `ProducerBatchSize: 10000` setting in `main.go`. Optimize it with the number of instances.

2. **Chunk Size**: There's a `ChunkSize: 8000` setting for snapshots. Adjust according to database load.

3. **Network**: All services must be on the same Docker network.

4. **PostgreSQL**: WAL (Write-Ahead Log) settings should be sufficient:
   ```
   wal_level=logical
   max_wal_senders=10
   max_replication_slots=10
   ```

## Example Scenarios

### Scenario 1: High Throughput Test
```bash
# Start with 5 instances
docker-compose up --scale go-pq-cdc-kafka=5 -d

# Add test data
docker-compose exec postgres psql -U cdc_user -d cdc_db -c \
  "INSERT INTO users (name) SELECT 'User' || i FROM generate_series(1, 1000000) AS i;"

# Monitor performance
docker stats
```

### Scenario 2: Graceful Scale Down
```bash
# First show existing instances
docker-compose ps go-pq-cdc-kafka

# Gradually scale down
docker-compose up --scale go-pq-cdc-kafka=3 -d
sleep 30
docker-compose up --scale go-pq-cdc-kafka=1 -d
```

### Scenario 3: Load Testing
```bash
# Test with different instance counts
for i in 1 2 3 5 10; do
  echo "Testing with $i instances..."
  docker-compose up --scale go-pq-cdc-kafka=$i -d
  sleep 60
  # Record metrics
done
```

## Resources

- Docker Compose Scale: https://docs.docker.com/compose/reference/up/
- Prometheus DNS SD: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#dns_sd_config
- Kafka Partitioning: https://kafka.apache.org/documentation/#intro_concepts_and_terms

