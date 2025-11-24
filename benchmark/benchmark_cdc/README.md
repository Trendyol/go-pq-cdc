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

### Result
|                      | go-pq-cdc  | Debezium     |
|----------------------|------------|--------------|
| Row Count            | 10 m       | 10 m         |
| Elapsed Time         | 2.5 min    | 21 min       |
| Cpu Usage Max        | 44%        | 181%         |
| Memory Usage Max     | 130 MB     | 1.07 GB      |
| Received Traffic Max | 4.36 MiB/s | 7.97 MiB/s   |
| Sent Traffic Max     | 5.96 MiB/s | 6.27 MiB/s   |

![10m_result](./10m_test.png)


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
     Username: `go-pq-cdc-user` Password: `go-pq-cdc-pass`
- Trace the process 
![benchmark_dashboard](./dashboard.png)

## Ports

- RedPanda Console: `8085` http://localhost:8085 
- RedPanda: `19092` http://localhost:19092
- Grafana: `3000` http://localhost:3000
- Prometheus: `9090`  http://localhost:9090
- cAdvisor: `8080` http://localhost:8080
- PostgreSQL:`5432` http://localhost:5432
- PostgreSQL Metric Exporter: `9187` http://localhost:9187 
- Debezium: `9093` http://localhost:9093
- go-pq-cdc Metric: `2112` http://localhost:2112

