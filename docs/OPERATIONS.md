# Build and Operations

## Requirements

- Zig 0.16.0
- uv for the Python development environment
- wolfSSL development library (`libwolfssl`)
- libuv development library (`libuv`)
- Network access on the first `-Dmimalloc=true` build so Zig can fetch the
  pinned mimalloc source dependency
- Redis or Valkey at runtime
- RabbitMQ at runtime

## Build

```bash
zig build
```

Artifacts are installed under `zig-out/bin/`.

Available Zig steps:

- `zig build`
- `zig build run-example -- [args...]`
- `zig build run-rabbitmq-console -- [args...]`
- `zig build run-rpc-control -- [args...]`
- `zig build -Dvalgrind=true valgrind-rpc-control -- [args...]`

Useful options:

- `-Dsanitizers=true`: enable ASan, UBSan, and LSan in debug builds.
- `-Dvalgrind=true`: use baseline CPU features for Valgrind compatibility.
- `-Dmimalloc=true`: fetch, statically link, and route allocation through
  mimalloc.
- `-Dstrip=true`: strip debug symbols.
- `-Drelease=true`: build in release mode.

## Local Quick Start

1. Set a unique `rpc_control.auth_token` in `conf/config.toml`.
2. Start Valkey and RabbitMQ:

   ```bash
   docker compose -f compose.yml up -d
   ```

3. Build the binaries:

   ```bash
   zig build
   ```

4. Start `rpc_control`:

   ```bash
   zig build run-rpc-control -- --config conf/config.toml
   ```

5. Use the [RPC API](RPC_API.md) or [Python client](PYTHON_CLIENT.md) to add
   monitored addresses.
6. Start the monitor:

   ```bash
   zig build run-example -- --config conf/config.toml
   ```

## Production Docker Stack

`compose.prod.yml` runs Valkey, RabbitMQ, `rpc_control`, and
`eth_mempool_monitor`.

Before starting, set a strong `rpc_control.auth_token` in `conf/config.toml` and
enable host overcommit for Valkey:

```bash
sudo sysctl -w vm.overcommit_memory=1
```

Build and start the stack:

```bash
docker build -t eth-mempool-monitor:latest .
docker compose -f compose.prod.yml up -d
```

Inspect status and logs:

```bash
docker compose -f compose.prod.yml ps
docker compose -f compose.prod.yml logs -f rpc_control eth_mempool_monitor
```

Watch RabbitMQ events with the console client:

```bash
docker exec -it rpc_control /usr/local/bin/rabbitmq_tx_console \
  --config /config/config.toml \
  --rabbitmq-host rabbitmq
```

## Diagnostics

Build for Valgrind and run Massif:

```bash
zig build -Dvalgrind=true -Dmimalloc=true -Drelease=true
valgrind --tool=massif --stacks=yes ./zig-out/bin/eth_mempool_monitor
ms_print massif.out.<pid>
```

## Convenience Commands

Initialize and activate the Python development environment:

```bash
uv venv --python 3.14
uv sync --only-dev
source .venv/bin/activate
```

The `justfile` provides shortcuts including:

- `just build`
- `just build-mimalloc`
- `just build-sanitize`
- `just build-release`
- `just format`
- `just python-tools`
- `just python-check`
- `just run`
- `just run-config`

Run `just --list` for the complete list.
