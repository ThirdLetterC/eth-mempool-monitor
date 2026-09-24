# Architecture

## Data Flow

1. `rpc_control` manages a Redis set (`redis.monitored_set_key`).
1. `eth_mempool_monitor` subscribes to `eth_subscribe` (`newPendingTransactions`).
1. If a provider returns only a transaction hash, the monitor requests the full
   transaction with `eth_getTransactionByHash`.
1. The monitor checks each transaction's `from` and `to` addresses in Redis.
1. Matching transactions are published to RabbitMQ as JSON.
1. Optionally, `http_transmitter` consumes those events and POSTs the original
   JSON bytes to a configured webhook.

## Application Modules

- `main.c` wires allocator selection, configuration, runtime execution, and
  centralized cleanup.
- `monitor_config.c` owns defaults, TOML loading, CLI parsing, validation,
  logging configuration, and sensitive string cleanup.
- `monitor_runtime.c` owns signal handling, integration configuration, and the
  bounded reconnect loop.
- `subscriber.c` owns WebSocket/Redis/RabbitMQ resource lifecycles and the
  receive loop.
- `subscriber_message.c` owns untrusted WebSocket message parsing, transaction
  lookup correlation, Redis membership checks, and matched-event publication.
- `rpc_control.c` is the RPC server process entry point and signal coordinator.
- `rpc_control_config.c` owns RPC CLI/TOML parsing, validation, logging setup,
  and configuration cleanup.
- `rpc_control_service.c` owns RPC authentication state, Redis commands, and
  JSON-RPC method dispatch.
- `http_transmitter_consumer.c` owns the manual-ack RabbitMQ consumer,
  delivery settlement, progress statistics, and bounded webhook worker pool.
- `http_transmitter_webhook.c` validates bounded JSON objects and owns libcurl
  retries, TLS verification, authentication, and HTTP status handling.

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

Publishing is asynchronous. The subscriber copies matched events into a
bounded ring and signals a dedicated libuv event loop. Its worker thread owns
the RabbitMQ connection, publishes up to 128 messages before draining
publisher confirms, and schedules reconnect backoff with a libuv timer.
Unconfirmed batches remain at the ring head for replay, providing at-least-once
delivery semantics. Queue admission, rather than broker confirmation, is the
success boundary returned to the subscriber.

The HTTP transmitter acknowledges an event only after a 2xx webhook response.
Transport and HTTP failures are retried with bounded exponential backoff and
then requeued. Permanently malformed input is rejected without requeue. This
also provides at-least-once delivery, so webhook processing must be idempotent.
Webhook requests may execute concurrently, but the connection-owning thread
alone consumes and settles RabbitMQ deliveries. Every worker owns its libcurl
easy handle, and the configured parallel request count bounds both threads and
copied in-flight payloads.

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
| Enqueue RabbitMQ publish | `O(t)` | `O(t)` | Copies into a bounded, preallocated descriptor ring. |
| Flush RabbitMQ batch | `O(b + sum(payload_i))` | `O(b)` | Publishes and confirms at most 128 messages per batch. |

Typical subscriber CPU work is `O(n + p + t)`. Because `p` is capped at 1024,
this is effectively `O(n + t)`. Broker I/O and confirm latency overlap with
subscriber ingestion on the publisher worker.

Fixed storage includes the 64 KiB receive buffer and 1024 lookup entries.
Dynamic storage is dominated by parsed JSON (`O(n)`), serialized events
(`O(t)`), and queued publish payloads. The outbound queue preallocates 4096
descriptors and is capped at 64 MiB of payload data.

The HTTP transmitter copies at most `webhook.parallel_requests` payloads of up
to 1 MiB each. Its worker and delivery-slot scans are `O(w)`, where `w <= 256`.

The RPC server accepts at most 256 concurrent clients. Each client may queue at
most 64 writes or 512 KiB of response data before the connection is closed.
The RabbitMQ console rejects payloads larger than 1 MiB before copying or
parsing them.

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
