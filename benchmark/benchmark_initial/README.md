## 10 M Insert Test

### Hardware

```txt
PC: Macbook Apple M1 Pro (2021)
Memory: 32 GB

go-pq-cdc:
  resources:
    limits:
      cpus: 1
      memory: 512M
    reservations:
      cpus: '0.25'
      memory: 128M

Debezium:
  resources:
    limits:
      cpus: 2
      memory: 1024M
    reservations:
      cpus: '0.25'
      memory: 128M
```

### Results

#### 1x Test

|                      | go-pq-cdc  | Debezium   |
| -------------------- | ---------- | ---------- |
| Row Count            | 10 m       | 10 m       |
| Elapsed Time         | 1 min      | 2 min      |
| Cpu Usage Max        | 69.9%      | 167%       |
| Memory Usage Max     | 44.6 MB    | 2.56 GB    |
| Received Traffic Max | 6.30 MiB/s | 4.85 MiB/s |
| Sent Traffic Max     | 15.4 MiB/s | 43.7 MiB/s |

![10m_1x](./10m_1x.png)

#### 3x Test

|                      | go-pq-cdc  | Debezium   |
| -------------------- | ---------- | ---------- |
| Row Count            | 10 m       | 10 m       |
| Elapsed Time         | 20 sec     | 2 min      |
| Cpu Usage Max        | 71%        | 187%       |
| Memory Usage Max     | 47.6 MB    | 2.43 GB    |
| Received Traffic Max | 6.30 MiB/s | 4.85 MiB/s |
| Sent Traffic Max     | 15.4 MiB/s | 46.1 MiB/s |

![10m_3x](./10m_3x.png)

## Requirements

- [Docker](https://docs.docker.com/compose/install/)
- [psql](https://www.postgresql.org/download/)

## Instructions

- Start the containers
  ```sh
  docker compose up -d
  ```
- Connect to Postgres database:
  ```sh
  psql postgres://cdc_user:cdc_pass@127.0.0.1:5432/cdc_db
  ```
- Insert data to users table:
  ```sql
  INSERT INTO users (name)
  SELECT
      'Oyleli' || i
  FROM generate_series(1, 1000000) AS i;
  ```
- Go to grafana dashboard: http://localhost:3000/d/edl1ybvsmc64gb/benchmark?orgId=1
  > **Grafana Credentials**  
  >  Username: `go-pq-cdc-user` Password: `go-pq-cdc-pass`

## Ports

- RedPanda Console: `8085` http://localhost:8085
- RedPanda: `19092` http://localhost:19092
- Grafana: `3000` http://localhost:3000
- Prometheus: `9090` http://localhost:9090
- cAdvisor: `8080` http://localhost:8080
- PostgreSQL:`5432` http://localhost:5432
- PostgreSQL Metric Exporter: `9187` http://localhost:9187
- Debezium: `9093` http://localhost:9093
- go-pq-cdc Metric: `2112` http://localhost:2112
