# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

ARG DEBIAN_IMAGE=debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251

FROM ${DEBIAN_IMAGE} AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH
ARG ZIG_VERSION=0.16.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        xz-utils \
        gcc \
        libc6-dev \
        pkg-config \
        libssl-dev \
        libwolfssl-dev \
        libuv1-dev \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
        amd64) \
            zig_arch=x86_64; \
            zig_sha256=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00 \
            ;; \
        arm64) \
            zig_arch=aarch64; \
            zig_sha256=ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17 \
            ;; \
        *) \
            echo "Unsupported architecture: ${TARGETARCH}" >&2; \
            exit 1 \
            ;; \
    esac \
    && zig_archive="zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz" \
    && curl --proto '=https' --tlsv1.2 -fsSLo "/tmp/${zig_archive}" \
        "https://ziglang.org/download/${ZIG_VERSION}/${zig_archive}" \
    && echo "${zig_sha256}  /tmp/${zig_archive}" | sha256sum --check --strict - \
    && tar -xJf "/tmp/${zig_archive}" -C /opt \
    && ln -s "/opt/zig-${zig_arch}-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig \
    && rm "/tmp/${zig_archive}"

WORKDIR /src
COPY . .

RUN zig build -Drelease=true -Dstrip=true -Dmimalloc=true

FROM ${DEBIAN_IMAGE} AS runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libwolfssl35 \
        libuv1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/zig-out/bin/eth_mempool_monitor /usr/local/bin/eth_mempool_monitor
COPY --from=builder /src/zig-out/bin/rpc_control /usr/local/bin/rpc_control
COPY --from=builder /src/zig-out/bin/rabbitmq_tx_console /usr/local/bin/rabbitmq_tx_console
COPY --from=builder /src/zig-out/bin/http_transmitter /usr/local/bin/http_transmitter
COPY --from=builder /src/zig-out/lib/libcurl.so /usr/local/lib/libcurl.so

ENV LD_LIBRARY_PATH=/usr/local/lib

USER nobody:nogroup

ENTRYPOINT ["/usr/local/bin/eth_mempool_monitor"]
CMD ["--config", "/config/config.toml"]
