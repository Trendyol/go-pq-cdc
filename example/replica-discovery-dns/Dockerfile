# Build context is the repository root, so the connector is built from the
# working tree (go.mod replaces the module with ../..), not a released version.
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build
COPY . .
WORKDIR /build/example/replica-discovery-dns
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -o /go/bin/connector ./main.go

FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=builder /go/bin/connector /app/connector
