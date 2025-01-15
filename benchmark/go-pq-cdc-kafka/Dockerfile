##- Build Stage
#FROM golang:1.22.2-alpine3.19 AS builder
#
#WORKDIR /go/src/github.com/vskurikhin/go-pq-cdc/benchmark
#
#COPY . .
#
#RUN go mod download
#RUN GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -a -v -o /go/bin/cdc  ./main.go
#
##- Run Stage
#FROM gcr.io/distroless/static-debian11
#
#WORKDIR /app
#
#COPY --from=builder /go/bin/cdc ./
#
#EXPOSE 8080
#
#CMD [ "./cdc" ]

#- Run Stage
FROM gcr.io/distroless/static-debian11

WORKDIR /app

COPY ../go-pq-cdc go-pq-cdc-kafka

EXPOSE 2112

CMD [ "./go-pq-cdc-kafka" ]