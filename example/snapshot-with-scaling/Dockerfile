#- Build Stage
FROM --platform=linux/arm64 golang:1.23-alpine3.19 AS builder

# Install git (required for go mod download)
RUN apk add --no-cache git

WORKDIR /build

# Copy the entire project (context is project root)
COPY . .

# Change to the example app directory
WORKDIR /build/example/snapshot-with-scaling

# Download dependencies
RUN go mod download

# Build the application
RUN CGO_ENABLED=0 go build -trimpath -a -v -o /go/bin/go-pq-cdc ./main.go

#- Run Stage
FROM --platform=linux/arm64 gcr.io/distroless/static-debian11

WORKDIR /app

COPY --from=builder /go/bin/go-pq-cdc ./

EXPOSE 2112

CMD [ "./go-pq-cdc" ]