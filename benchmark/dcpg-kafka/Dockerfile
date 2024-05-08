##- Build Stage
#FROM golang:1.22.2-alpine3.19 AS builder
#
#WORKDIR /go/src/github.com/3n0ugh/dcpg/benchmark
#
#COPY . .
#
#RUN go mod download
#RUN GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -trimpath -a -v -o /go/bin/dcpg  ./main.go
#
##- Run Stage
#FROM gcr.io/distroless/static-debian11
#
#WORKDIR /app
#
#COPY --from=builder /go/bin/dcpg ./
#
#EXPOSE 8080
#
#CMD [ "./dcpg" ]

#- Run Stage
FROM gcr.io/distroless/static-debian11

WORKDIR /app

COPY dcpg dcpg

EXPOSE 2112

CMD [ "./dcpg" ]