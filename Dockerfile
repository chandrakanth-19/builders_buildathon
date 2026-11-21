# Multi-stage build for ultra-fast native execution
FROM rust:bookworm AS builder

WORKDIR /build
COPY engine/Cargo.toml engine/Cargo.lock ./
COPY engine/src ./src
RUN cargo build --release

# Slim runtime container
FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

WORKDIR /app
COPY --from=builder /build/target/release/engine /app/engine
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh /app/engine

ENV TMPDIR=/scratch \
    HOME=/scratch

CMD ["/app/run.sh"]
