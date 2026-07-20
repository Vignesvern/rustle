# syntax=docker/dockerfile:1

# ---- builder ----
FROM rust:1-slim-bookworm AS builder
# ring (our TLS crypto backend) needs a C compiler; pkg-config for good measure.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --locked

# ---- runtime ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/rustle /usr/local/bin/rustle
# The web client is served from ./static at runtime (migrations are embedded in the binary).
COPY static ./static
ENV RUSTLE_ADDR=0.0.0.0:8080
EXPOSE 8080
CMD ["rustle"]
