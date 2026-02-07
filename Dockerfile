# syntax=docker/dockerfile:1.7

FROM debian:bookworm-slim AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG ZIG_VERSION=0.15.2

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        xz-utils \
        gcc \
        libc6-dev \
        pkg-config \
        libwolfssl-dev \
        libuv1-dev \
        libmimalloc-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && ln -s "/opt/zig-x86_64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig

WORKDIR /src
COPY . .

RUN zig build -Drelease=true -Dstrip=true -Dmimalloc=true

FROM debian:bookworm-slim AS runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libwolfssl35 \
        libmimalloc2.0 \
        libuv1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /src/zig-out/bin/eth_mempool_monitor /usr/local/bin/eth_mempool_monitor
COPY --from=builder /src/zig-out/bin/rpc_control /usr/local/bin/rpc_control
COPY --from=builder /src/zig-out/bin/rabbitmq_tx_console /usr/local/bin/rabbitmq_tx_console

ENV CONFIG_PATH=/config/config.toml

VOLUME ["/config"]

USER nobody:nogroup

ENTRYPOINT ["/usr/local/bin/eth_mempool_monitor"]
CMD ["--config", "/config/config.toml"]
