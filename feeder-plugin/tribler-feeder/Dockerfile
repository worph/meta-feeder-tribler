# syntax=docker/dockerfile:1
# =============================================================================
# tribler-feeder — Tribler (IPv8 overlay) feeder sidecar. Multi-stage build.
#
# Build context must be this repo root (packages/plugins/meta-feeder-tribler) so
# the `meta-feeder-sdk` path dependency resolves. Same BuildKit cache-mount
# strategy as the other feeders. No librqbit here (tribler enumerates files via
# the Tribler core's metainfo), so the cold build is lighter than the torrent
# feeder's.
# =============================================================================

FROM rust:1.89-slim-bookworm AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
COPY feeder-plugin/ feeder-plugin/

RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=meta-feeder-tribler-target,target=/build/target \
    cargo build --release --bin tribler-feeder \
    && cp target/release/tribler-feeder /tribler-feeder

# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tribler-feeder /usr/local/bin/tribler-feeder

RUN useradd --system --uid 10001 --no-create-home metafeeder \
    && mkdir -p /data/meta-feeder \
    && chown metafeeder:metafeeder /data/meta-feeder

USER metafeeder
WORKDIR /data/meta-feeder

EXPOSE 8080

ENV META_FEEDER_HTTP_LISTEN=0.0.0.0:8080 \
    META_FEEDER_STATE_DIR=/data/meta-feeder \
    RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/tribler-feeder"]
