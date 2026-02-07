# ETH Mempool Monitor

`eth-mempool-monitor` subscribes to Ethereum pending transactions over WebSocket, filters them against a monitored address set stored in Redis/Valkey, and publishes matching transactions to RabbitMQ.

The project builds three binaries:

- `eth_mempool_monitor`: WebSocket subscriber + Redis filter + RabbitMQ publisher.
- `rpc_control`: newline-delimited JSON-RPC TCP server used to manage monitored addresses in Redis (token-authenticated).
- `rabbitmq_tx_console`: RabbitMQ consumer that prints monitored-transaction events in human-readable form.

## Architecture

1. `rpc_control` manages a Redis set (`redis.monitored_set_key`).
2. `eth_mempool_monitor` subscribes to `eth_subscribe` (`newPendingTransactions`).
3. If a provider returns only a tx hash, `eth_mempool_monitor` requests the full tx via `eth_getTransactionByHash`.
4. For each tx object, it checks `from`/`to` addresses in Redis.
5. Matching transactions are published to RabbitMQ as JSON.

## Algorithmic Complexity (`eth_mempool_monitor`)

Symbols used below:

- `n`: incoming websocket message size in bytes (bounded by 64 KiB receive buffer).
- `p`: pending tx lookup table occupancy (`p <= 1024`).
- `q`: RabbitMQ replay queue depth (`q <= 4096`).
- `t`: transaction/event JSON size in bytes.

Per-message processing complexity:

| Stage | Time | Extra Space | Notes |
|---|---:|---:|---|
| Receive websocket frame(s) | `O(n)` | `O(1)` | Reads/discards payload bytes linearly. |
| Parse JSON message | `O(n)` | `O(n)` | Parsing allocates a JSON tree proportional to message size. |
| Hash-only notification path (`eth_getTransactionByHash` request enqueue) | `O(p)` | `O(1)` | Linear scan for a free pending-lookup slot in a fixed array. |
| Lookup-response correlation by request id | `O(p)` | `O(1)` | Linear scan over pending lookup entries. |
| Address normalization (`from`/`to`) | `O(1)` | `O(1)` | At most two fixed-length Ethereum addresses. |
| Redis membership checks (`SISMEMBER`, up to 2 keys) | `O(1)` local CPU | `O(1)` | Constant number of commands; Redis set lookup is expected `O(1)` average. |
| Build + serialize RabbitMQ event payload | `O(t)` | `O(t)` | Deep-copies tx JSON and serializes resulting event JSON. |
| RabbitMQ replay enqueue | `O(t)` | `O(t)` | Copies payload into replay queue when replay/backoff is active. |
| RabbitMQ replay flush | Worst case `O(q + sum(payload_i))` | `O(1)` | Ring-buffer replay queue drains without per-pop shifts. |

End-to-end CPU complexity:

- Typical steady state (small/empty replay queue): linear in payload size, approximately `O(n + p + t)`; with fixed cap `p <= 1024`, this is effectively `O(n + t)`.
- Degraded broker/backlog case: dominated by replay draining, worst case `O(n + p + q + sum(payload_i))`.

Memory complexity:

- Fixed buffers/arrays are bounded (`64 KiB` receive buffer, `1024` pending lookups).
- Dynamic memory is dominated by:
  - parsed JSON tree: `O(n)`,
  - serialized publish payload: `O(t)`,
  - replay queue storage: `O(sum(payload_i))` for up to `4096` queued messages.
  - in steady state, RabbitMQ publish path avoids replay payload copies unless retry/backoff is active.

## Requirements

- Zig (build system)
- wolfSSL development library (`libwolfssl`)
- libuv development library (`libuv`)
- mimalloc development library (`libmimalloc`) when building with `-Dmimalloc=true`
- Redis/Valkey (runtime)
- RabbitMQ (runtime)

## Build

```bash
zig build
```

Available Zig steps:

- `zig build` (default install)
- `zig build run-example -- [args...]`
- `zig build run-rabbitmq-console -- [args...]`
- `zig build run-rpc-control -- [args...]`
- `zig build -Dvalgrind=true valgrind-rpc-control -- [args...]`

Useful build options:

- `-Dsanitizers=true` to enable ASan/UBSan/LSan in Debug builds
- `-Dvalgrind=true` to force baseline CPU features for Valgrind compatibility
- `-Dmimalloc=true` to route `malloc`/`calloc`/`realloc`/`free` through mimalloc
- `-Dstrip=true` to strip debug symbols from built artifacts

Artifacts are installed under `zig-out/bin/`.

## Quick Start

1. Start Redis/Valkey + RabbitMQ:

```bash
docker compose -f compose.yml up -d
```

2. Build binaries:

```bash
zig build
```

3. Start control server (terminal 1):

```bash
# Set rpc_control.auth_token in config.toml to a unique secret first.
zig build run-rpc-control -- --config config.toml
```

Valgrind Massif profiling example:

```bash
zig build -Dvalgrind=true -Dmimalloc=true -Drelease=true

valgrind --tool=massif --stacks=yes ./zig-out/bin/eth_mempool_monitor

ms_print massif.out.<pid>
```

4. Add monitored addresses (terminal 2):

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"auth","params":{"token":"<rpc_control.auth_token>"}}' | nc 127.0.0.1 8080
printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"monitor_add","params":{"address":"0x1111111111111111111111111111111111111111"}}' | nc 127.0.0.1 8080
printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"monitor_list"}' | nc 127.0.0.1 8080
```

5. Start mempool monitor (terminal 3):

```bash
zig build run-example -- --config config.toml
```

## Production Docker Stack

`compose.prod.yml` runs the full stack:

- `valkey`: monitored address store.
- `rabbitmq`: queue broker (AMQP `5672` only; management UI disabled in production).
- `rpc_control`: JSON-RPC API for address management.
- `eth_mempool_monitor`: websocket subscriber + publisher.

Before starting:

- Set a strong `rpc_control.auth_token` in `config.toml`.
- Ensure host overcommit is enabled for Valkey:

```bash
sudo sysctl -w vm.overcommit_memory=1
```

Build and start:

```bash
docker build -t eth-mempool-monitor:latest .
docker compose -f compose.prod.yml up -d
```

Check status/logs:

```bash
docker compose -f compose.prod.yml ps
docker compose -f compose.prod.yml logs -f rpc_control eth_mempool_monitor
```

### Monitoring Workflow For Users

1. Authenticate to `rpc_control`:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"auth","params":{"token":"<rpc_control.auth_token>"}}' | nc 127.0.0.1 8080
```

2. Add addresses to monitor:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"monitor_add","params":{"address":"0x1111111111111111111111111111111111111111"}}' | nc 127.0.0.1 8080
printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"monitor_list"}' | nc 127.0.0.1 8080
```

3. Watch monitor output:

```bash
docker compose -f compose.prod.yml logs -f eth_mempool_monitor
```

4. Optional: watch RabbitMQ transaction events from the container image:

```bash
docker exec -it rpc_control /usr/local/bin/rabbitmq_tx_console --config /config/config.toml --rabbitmq-host rabbitmq
```

## Configuration

Configuration details were moved to [`CONFIG.md`](CONFIG.md).

## `rpc_control` JSON-RPC API

Transport is raw TCP on `<rpc_control.host>:<rpc_control.port>` with one JSON-RPC request per line.
Default bind host is `127.0.0.1`.
All methods except `ping` require authenticating first via `auth`.

Methods:

- `ping`
- `auth` (requires `{"token":"<rpc_control.auth_token>"}`)
- `health`
- `methods`
- `monitor_add` (aliases: `add_address`, `add_addresses`)
- `monitor_remove` (aliases: `remove_address`, `remove_addresses`)
- `monitor_has` (alias: `is_monitored`)
- `monitor_count`
- `monitor_list`
- `monitor_clear` (requires `{"confirm": true}`)

`monitor_add` / `monitor_remove` / `monitor_has` accept:

- a single string address
- an array of string addresses
- an object with `address` or `addresses`

Example:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":10,"method":"auth","params":{"token":"<rpc_control.auth_token>"}}' | nc 127.0.0.1 8080
printf '%s\n' '{"jsonrpc":"2.0","id":11,"method":"monitor_has","params":{"address":"0x1111111111111111111111111111111111111111"}}' | nc 127.0.0.1 8080
```

## RabbitMQ Event Payload

When a monitored address matches `from` or `to`, the monitor publishes JSON like:

Publishing uses RabbitMQ publisher confirms with in-process replay retries
(at-least-once delivery semantics).

```json
{
  "hash": "0x...",
  "from": "0x...",
  "to": "0x...",
  "from_monitored": true,
  "to_monitored": false,
  "transaction": {
    "...": "full transaction object from eth_subscription result"
  }
}
```

## Python RPC Client

A Python client library is provided for easy interaction with the `rpc_control` server.
For full documentation, see [`PYTHON_CLIENT.md`](PYTHON_CLIENT.md).

### Installation

No additional dependencies required - uses only Python standard library.

### Basic Usage

```python
from rpc_client import RPCClient

# Connect to the RPC server
with RPCClient(host='127.0.0.1', port=8080, auth_token='your-secure-token') as client:
    # Add an address to monitor
    client.monitor_add('0x1111111111111111111111111111111111111111')
    
    # List all monitored addresses
    addresses = client.monitor_list()
    
    # Check if an address is monitored
    is_monitored = client.monitor_has('0x1111111111111111111111111111111111111111')
    
    # Remove an address
    client.monitor_remove('0x1111111111111111111111111111111111111111')
```

### Running the Example

```bash
# Terminal 1: Start the RPC control server
zig build run-rpc-control -- --config config.toml

# Terminal 2: Run the example script
python3 example_rpc_client.py

# Or use the client interactively
RPC_CONTROL_AUTH_TOKEN=your-secure-token python3 rpc_client.py
```

### Available Methods

- `ping()` - Test server connectivity
- `authenticate(token)` - Authenticate the current TCP connection
- `health()` - Get server health status
- `methods()` - List available RPC methods
- `monitor_add(address)` - Add address(es) to monitoring set
- `monitor_remove(address)` - Remove address(es) from monitoring set
- `monitor_has(address)` - Check if address(es) are monitored
- `monitor_count()` - Get count of monitored addresses
- `monitor_list()` - Get list of all monitored addresses
- `monitor_clear(confirm=True)` - Clear all monitored addresses

## Convenience Commands

The repo also includes `justfile` shortcuts:

- `just build`
- `just build-mimalloc`
- `just build-sanitize`
- `just build-release`
- `just run`
- `just run-config`

## Acknowledgements

- Zig (https://ziglang.org/)
- wolfSSL (https://www.wolfssl.com/)
- libuv (https://libuv.org/)
- mimalloc (https://microsoft.github.io/mimalloc/)
- Redis (https://redis.io/) / Valkey (https://valkey.io/)
- RabbitMQ (https://www.rabbitmq.com/)

Libraries:

- https://github.com/ThirdLetterC/jsonrpc
- https://github.com/ThirdLetterC/hiredis
- https://github.com/ThirdLetterC/rabbitmq
- https://github.com/ThirdLetterC/websocket-client
- https://github.com/ThirdLetterC/parson
- https://github.com/ThirdLetterC/parg
- https://github.com/ThirdLetterC/toml
- https://github.com/ThirdLetterC/ulog
