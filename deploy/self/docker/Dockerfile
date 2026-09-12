FROM public.ecr.aws/docker/library/node:lts-trixie-slim AS console

WORKDIR /source
COPY Cargo.toml ./
WORKDIR /source/console
RUN corepack enable
COPY console/package.json console/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY console/ ./
# Docker 构建上下文不含 .git，由 build.sh 将同一标识传给前端和二进制。
ARG GPROXY_BUILD_HASH=unknown
RUN GPROXY_BUILD_HASH="$GPROXY_BUILD_HASH" pnpm build

FROM public.ecr.aws/docker/library/rust:1-trixie AS builder

WORKDIR /source
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake libclang-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
COPY --from=console /source/console/dist/ crates/gproxy-host-axum/assets/web/
# 由 build.sh 传入；显式导出给 build.rs，确保二进制与前端使用同一标识。
ARG CARGO_PROFILE_RELEASE_OPT_LEVEL
ARG CARGO_PROFILE_RELEASE_LTO
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS
ARG CARGO_PROFILE_RELEASE_PANIC
ARG CARGO_PROFILE_RELEASE_STRIP
ARG CARGO_PROFILE_RELEASE_DEBUG
ARG CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS
ARG CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS
ARG CARGO_PROFILE_RELEASE_INCREMENTAL
ARG RUSTFLAGS
ARG GPROXY_BUILD_HASH=unknown
RUN GPROXY_BUILD_HASH="$GPROXY_BUILD_HASH" cargo build --locked --release --target x86_64-unknown-linux-gnu \
    --package gproxy-host-axum --bin gproxy

FROM debian:13-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 /app/data

COPY --from=builder --chmod=755 /source/target/x86_64-unknown-linux-gnu/release/gproxy /usr/local/bin/gproxy

USER 65532:65532
EXPOSE 8787
ENV GPROXY_HOST=0.0.0.0 \
    GPROXY_PORT=8787 \
    GPROXY_DATA_DIR=/app/data \
    GPROXY_PERSISTENCE=sqlite
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/gproxy"]
