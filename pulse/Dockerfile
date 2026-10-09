# Build stage: compile a static binary.
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /pulse ./cmd/pulse

# Runtime stage: minimal image with just the binary.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /pulse /usr/local/bin/pulse
ENTRYPOINT ["pulse"]
