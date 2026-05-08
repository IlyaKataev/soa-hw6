FROM golang:1.26-trixie AS builder
ARG SERVICE
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates git bash \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN test -n "$SERVICE" && go build -o /bin/service ./cmd/${SERVICE}

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/service /service
ENTRYPOINT ["/service"]
