# syntax=docker/dockerfile:1
# skaidb — single static binary, built from source (musl, no runtime deps).
#
#   docker build -f docker/Dockerfile -t skaidb .
#   docker run -p 7000:7000 -p 7080:7080 -v skaidb-data:/var/lib/skaidb skaidb
#
# Every configuration option is an environment variable (SKAIDB_*) — see
# docker/README.md / docs/DOCKER.md for the full list, or run:
#   docker run --rm skaidb --help

FROM rust:1.93-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release -p skaidb-server -p skaidb-cli \
 && cp target/release/skaidb target/release/skaidbsh /usr/local/bin/

FROM alpine:3.20
RUN apk add --no-cache wget \
 && addgroup -S skaidb && adduser -S -G skaidb skaidb \
 && mkdir -p /var/lib/skaidb /etc/skaidb \
 && chown -R skaidb:skaidb /var/lib/skaidb /etc/skaidb
COPY --from=build /usr/local/bin/skaidb /usr/local/bin/skaidbsh /usr/local/bin/

USER skaidb
VOLUME /var/lib/skaidb
# binary protocol / REST+UI+ES+PromQL / internode / dedicated metrics
EXPOSE 7000 7080 7100 9090

ENV SKAIDB_DATA_DIR=/var/lib/skaidb \
    SKAIDB_BIND_ADDR=0.0.0.0 \
    SKAIDB_MEMORY_TARGET=auto

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s \
  CMD wget -q -O /dev/null "http://127.0.0.1:${SKAIDB_REST_PORT:-7080}/ready" || exit 1

# A mounted /etc/skaidb/skaidb.toml is picked up when SKAIDB_CONFIG points
# at it; env vars and CLI flags override the file (env > file > defaults).
ENTRYPOINT ["/usr/local/bin/skaidb"]
