# go-pgx-cdc

go-pgx-cdc is designed to provide efficient and lightweight Change Data Capture (CDC) for PostgreSQL databases.
The architecture leverages PostgreSQL's built-in logical replication capabilities to capture changes in the database and
stream these changes to downstream systems, such as Kafka, Elasticsearch etc. The entire system is written in Golang,
ensuring low resource consumption and high performance.
