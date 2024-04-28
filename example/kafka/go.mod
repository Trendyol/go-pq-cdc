module github.com/3n0ugh/dcpg/example/kafka

go 1.22.1

replace github.com/3n0ugh/dcpg => ../..

require (
	github.com/3n0ugh/dcpg v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/go-playground/errors v3.3.0+incompatible // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20231201235250-de7065d80cb9 // indirect
	github.com/jackc/pgx/v5 v5.5.5 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)