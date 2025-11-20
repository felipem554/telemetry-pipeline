FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o ingest-api ./cmd

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/ingest-api /usr/local/bin/ingest-api
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ingest-api"]
