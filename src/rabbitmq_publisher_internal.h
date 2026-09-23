#pragma once

#include "websocket-client/rabbitmq_publisher.h"

#include "rabbitmq/amqp.h"

#include <stddef.h>
#include <stdint.h>

typedef struct ws_rabbitmq_replay_message ws_rabbitmq_replay_message_t;
struct ws_rabbitmq_replay_message {
  char *payload;
  size_t payload_length;
};

struct ws_rabbitmq_publisher {
  amqp_connection_state_t connection;
  amqp_channel_t channel;
  const char *host;
  const char *username;
  const char *password;
  const char *vhost;
  const char *queue;
  char *host_owned;
  char *username_owned;
  char *password_owned;
  char *vhost_owned;
  char *queue_owned;
  uint16_t port;
  uint16_t heartbeat_seconds;
  amqp_bytes_t queue_bytes;
  amqp_basic_properties_t publish_properties;
  bool queue_durable;
  bool logged_in;
  bool channel_open;
  ws_rabbitmq_replay_message_t *replay_queue;
  size_t replay_count;
  size_t replay_capacity;
  size_t replay_head;
  uint64_t retry_not_before_ms;
  uint32_t retry_backoff_ms;
  uint32_t skipped_flush_attempts;
  bool replay_queue_full_logged;
  uint64_t replay_queue_dropped_messages;
};

[[nodiscard]] bool
ws_rabbitmq_connection_open(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_connection_close(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] bool
ws_rabbitmq_connection_publish(ws_rabbitmq_publisher_t *publisher,
                               const char *payload, size_t payload_length);
[[nodiscard]] bool
ws_rabbitmq_retry_allows_flush(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_retry_record_failure(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_retry_record_success(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_replay_clear(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] bool
ws_rabbitmq_replay_enqueue(ws_rabbitmq_publisher_t *publisher,
                           const char *payload, size_t payload_length);
[[nodiscard]] bool ws_rabbitmq_replay_flush(ws_rabbitmq_publisher_t *publisher);
