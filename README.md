# ETH Mempool Monitor

[![Compile](https://github.com/ThirdLetterC/eth-mempool-monitor/actions/workflows/compile.yml/badge.svg)](https://github.com/ThirdLetterC/eth-mempool-monitor/actions/workflows/compile.yml)
[![Python quality](https://github.com/ThirdLetterC/eth-mempool-monitor/actions/workflows/python.yml/badge.svg)](https://github.com/ThirdLetterC/eth-mempool-monitor/actions/workflows/python.yml)

`eth-mempool-monitor` subscribes to Ethereum pending transactions over WebSocket,
filters them against addresses stored in Redis/Valkey, and publishes matching
transactions to RabbitMQ.

The project builds three binaries:

- `eth_mempool_monitor`: WebSocket subscriber, Redis filter, and RabbitMQ publisher.
- `rpc_control`: authenticated JSON-RPC server for managing monitored addresses.
- `rabbitmq_tx_console`: console consumer for monitored-transaction events.

## Quick Start

Requirements and complete setup instructions are in
[the operations guide](docs/OPERATIONS.md).

Set a unique `rpc_control.auth_token` in `conf/config.toml`, then run:

```bash
docker compose -f compose.yml up -d
zig build
zig build run-rpc-control -- --config conf/config.toml
```

In another terminal, start the monitor:

```bash
zig build run-example -- --config conf/config.toml
```

Use the [RPC API](docs/RPC_API.md) or the
[Python client](docs/PYTHON_CLIENT.md) to add addresses to the monitored set.

## Documentation

- [Architecture and complexity](docs/ARCHITECTURE.md)
- [Build, development, and operations](docs/OPERATIONS.md)
- [Configuration reference](docs/CONFIG.md)
- [`rpc_control` JSON-RPC API](docs/RPC_API.md)
- [Python RPC client](docs/PYTHON_CLIENT.md)

## Development

Create the Python development environment with
[uv](https://docs.astral.sh/uv/):

```bash
uv venv --python 3.13
uv sync --only-dev
source .venv/bin/activate
```

Common commands:

```bash
just build
just build-sanitize
just format
just check-c-format
just python-check
```

Run `just --list` to see all available shortcuts. Build artifacts are installed
under `zig-out/bin/`.

## License

See [LICENSE](LICENSE).
