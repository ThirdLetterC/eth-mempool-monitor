#pragma once

/**
 * Internal subscriber message-processing boundary.
 * WebSocket messages and Redis replies are untrusted; parsing, correlation,
 * address validation, and bounded pending lookup state live behind this API.
 */

#include "hiredis/hiredis.h"
#include "websocket-client/rabbitmq_publisher.h"
#include "websocket-client/ws_client.h"

#include <stdint.h>

constexpr size_t WS_SUBSCRIBER_TX_HASH_CAPACITY = 67;
constexpr size_t WS_SUBSCRIBER_MAX_PENDING_TX_LOOKUPS = 1024;
constexpr uint64_t WS_SUBSCRIBER_INITIAL_TX_LOOKUP_REQUEST_ID = 1'000'000;

typedef struct ws_subscriber_pending_tx_lookup
    ws_subscriber_pending_tx_lookup_t;
struct ws_subscriber_pending_tx_lookup {
  bool in_use;
  uint64_t request_id;
  char tx_hash[WS_SUBSCRIBER_TX_HASH_CAPACITY];
};

typedef struct ws_subscriber_retry_gate ws_subscriber_retry_gate_t;
struct ws_subscriber_retry_gate {
  uint64_t next_retry_at_ms;
  uint32_t backoff_ms;
  uint32_t skipped_attempts;
};

typedef struct ws_subscriber_runtime_config ws_subscriber_runtime_config_t;
struct ws_subscriber_runtime_config {
  ws_client_t *client;
  redisContext *redis;
  const char *monitored_set_key;
  ws_rabbitmq_publisher_t *rabbitmq_publisher;
  ws_subscriber_retry_gate_t redis_retry_gate;
  uint64_t next_tx_lookup_request_id;
  size_t pending_tx_lookup_count;
  ws_subscriber_pending_tx_lookup_t
      pending_tx_lookups[WS_SUBSCRIBER_MAX_PENDING_TX_LOOKUPS];
};

typedef enum {
  WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE = 0,
  WS_SUBSCRIBER_MESSAGE_ACTION_RECONNECT = 1,
} ws_subscriber_message_action_t;

[[nodiscard]] ws_subscriber_message_action_t
ws_subscriber_handle_message(const char *message, size_t message_length,
                             ws_subscriber_runtime_config_t *runtime_config);
