# Runtime image for lark-server.
#
# Multi-stage so the final image doesn't carry the Rust toolchain. The
# build stage compiles a release binary against the workspace, then we
# copy just the binary into a slim runtime image.
#
# Build context is the repo root (`.`) so the workspace Cargo.toml plus
# the `blob/` and `server/` crates are all visible to the builder.

# --- Stage 1: build ----------------------------------------------------------
# Recipe mirrors Dockerfile.dev exactly — glommio's build.rs is sensitive
# to changes in the toolchain environment, so we keep it identical to what
# the dev container uses.
FROM ubuntu:22.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    pkg-config \
    libssl-dev \
    liburing-dev \
    mold \
    clang \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src
COPY Cargo.toml /src/Cargo.toml
COPY blob /src/blob
COPY server /src/server
# Cargo loads every workspace member's manifest even when we ask it to
# build a single `-p lark-server`, so the tools/ tree has to be present.
COPY tools /src/tools

RUN cargo build --release -p lark-server

# --- Stage 2: runtime --------------------------------------------------------
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    liburing2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /src/target/release/lark-server /app/lark-server

# Default proxy port — lark-edge connects here over TCP.
EXPOSE 2727

# Per-database WAL + blob files + sequence files live here.
VOLUME ["/data"]

ENTRYPOINT ["/app/lark-server"]
