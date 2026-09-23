# Architecture

## Data Flow

1. `rpc_control` manages a Redis set (`redis.monitored_set_key`).
2. `eth_mempool_monitor` subscribes to `eth_subscribe` (`newPendingTransactions`).
3. If a provider returns only a transaction hash, the monitor requests the full
   transaction with `eth_getTransactionByHash`.
4. The monitor checks each transaction's `from` and `to` addresses in Redis.
5. Matching transactions are published to RabbitMQ as JSON.

## Application Modules

- `main.c` wires allocator selection, configuration, runtime execution, and
  centralized cleanup.
- `monitor_config.c` owns defaults, TOML loading, CLI parsing, validation,
  logging configuration, and sensitive string cleanup.
- `monitor_runtime.c` owns signal handling, integration configuration, and the
  bounded reconnect loop.

## RabbitMQ Event Payload

When a monitored address matches `from` or `to`, the monitor publishes JSON like:

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

Publishing uses RabbitMQ publisher confirms with in-process replay retries,
providing at-least-once delivery semantics.

## Algorithmic Complexity

Symbols used below:

- `n`: incoming WebSocket message size in bytes, bounded by a 64 KiB receive buffer.
- `p`: pending transaction lookup table occupancy (`p <= 1024`).
- `q`: RabbitMQ replay queue depth (`q <= 4096`).
- `t`: transaction/event JSON size in bytes.

| Stage | Time | Extra space | Notes |
|---|---:|---:|---|
| Receive WebSocket frames | `O(n)` | `O(1)` | Reads or discards payload bytes linearly. |
| Parse JSON message | `O(n)` | `O(n)` | Allocates a tree proportional to message size. |
| Enqueue hash-only lookup | `O(p)` | `O(1)` | Scans a fixed array for a free lookup slot. |
| Correlate lookup response | `O(p)` | `O(1)` | Scans pending entries by request ID. |
| Normalize `from` and `to` | `O(1)` | `O(1)` | Handles at most two fixed-length addresses. |
| Redis membership checks | `O(1)` local CPU | `O(1)` | Runs at most two `SISMEMBER` commands. |
| Build and serialize event | `O(t)` | `O(t)` | Copies the transaction JSON into the event. |
| Enqueue RabbitMQ replay | `O(t)` | `O(t)` | Copies the payload while replay/backoff is active. |
| Flush RabbitMQ replay | `O(q + sum(payload_i))` worst case | `O(1)` | Drains a ring buffer without per-pop shifts. |

Typical steady-state CPU work is `O(n + p + t)`. Because `p` is capped at
1024, this is effectively `O(n + t)`. During a broker backlog, replay draining
can dominate at `O(n + p + q + sum(payload_i))`.

Fixed storage includes the 64 KiB receive buffer and 1024 lookup entries.
Dynamic storage is dominated by parsed JSON (`O(n)`), serialized events (`O(t)`),
and up to 4096 queued replay payloads (`O(sum(payload_i))`).

## Dependencies

- [Zig](https://ziglang.org/) for the build system
- [wolfSSL](https://www.wolfssl.com/)
- [libuv](https://libuv.org/)
- [mimalloc](https://microsoft.github.io/mimalloc/), optionally
- [Redis](https://redis.io/) or [Valkey](https://valkey.io/)
- [RabbitMQ](https://www.rabbitmq.com/)

Vendored libraries originate from these ThirdLetterC repositories:

- [jsonrpc](https://github.com/ThirdLetterC/jsonrpc)
- [hiredis](https://github.com/ThirdLetterC/hiredis)
- [rabbitmq](https://github.com/ThirdLetterC/rabbitmq)
- [websocket-client](https://github.com/ThirdLetterC/websocket-client)
- [parson](https://github.com/ThirdLetterC/parson)
- [parg](https://github.com/ThirdLetterC/parg)
- [toml](https://github.com/ThirdLetterC/toml)
- [ulog](https://github.com/ThirdLetterC/ulog)
