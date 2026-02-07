# Configuration

Default config file path is `config.toml`. CLI flags override TOML values.

## Pending Transaction Fallback

Some RPC providers return hash-only notifications for `newPendingTransactions` even when the subscription includes the `true` detail flag.

When that happens, the monitor automatically sends `eth_getTransactionByHash` over the same websocket connection and runs the normal Redis/RabbitMQ pipeline once the full tx object is returned.

## `eth_mempool_monitor` keys

- `connection.host`
- `connection.port`
- `connection.path`
- `connection.secure`
- `connection.read_timeout_seconds`
- `connection.write_timeout_seconds`
- `subscription.request`
- `retry.enabled`
- `retry.initial_backoff_ms`
- `retry.max_backoff_ms`
- `redis.host`
- `redis.port`
- `redis.monitored_set_key`
- `rabbitmq.host`
- `rabbitmq.port`
- `rabbitmq.username`
- `rabbitmq.password`
- `rabbitmq.vhost`
- `rabbitmq.queue`
- `rabbitmq.queue_durable`
- `rabbitmq.channel`
- `rabbitmq.heartbeat_seconds`
- `rabbitmq.enabled`
- `logging.level` (`trace|debug|info|warn|error|fatal`)

## `rabbitmq_tx_console` keys

- `rabbitmq.host`
- `rabbitmq.port`
- `rabbitmq.username`
- `rabbitmq.password`
- `rabbitmq.vhost`
- `rabbitmq.queue`
- `rabbitmq.queue_durable`
- `rabbitmq.channel`
- `rabbitmq.heartbeat_seconds`
- `rabbitmq_consumer.read_timeout_seconds`
- `rabbitmq_consumer.prefetch_count`
- `rabbitmq_consumer.auto_ack`

Useful flags:

- `--config <path>`
- `--secure` / `--insecure`
- `--redis-host`, `--redis-port`, `--redis-key`
- `--rabbitmq-host`, `--rabbitmq-port`, `--rabbitmq-user`, `--rabbitmq-password`, `--rabbitmq-vhost`, `--rabbitmq-queue`
- `--rabbitmq-queue-durable` / `--rabbitmq-queue-transient`

## `rpc_control` keys

- `rpc_control.host`
- `rpc_control.port`
- `rpc_control.backlog`
- `rpc_control.auth_token`
- `redis.host`
- `redis.port`
- `redis.monitored_set_key`

Useful flags:

- `--config <path>`
- `--listen-host <host>`
- `--port <port>`
- `--backlog <count>`
- `--auth-token <token>`
- `--redis-host <host>`
- `--redis-port <port>`
- `--redis-key <key>`
