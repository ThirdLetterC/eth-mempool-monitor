#pragma once

#include "websocket-client/rabbitmq_publisher.h"

#include "rabbitmq/amqp.h"

#include <stddef.h>
#include <stdint.h>
#include <uv.h>

/*
 * Private publisher state shared by lifecycle, connection, and replay modules.
 * All *_owned pointers belong to the publisher. Broker handles are valid only
 * while their corresponding logged_in/channel_open flags are set.
 */
typedef struct ws_rabbitmq_replay_message ws_rabbitmq_replay_message_t;
struct ws_rabbitmq_replay_message {
  /* Owned payload copy retained until publish succeeds or the queue clears. */
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
  uint64_t next_publish_sequence;

  /*
   * The caller and publisher event-loop thread share only this bounded queue.
   * The AMQP connection and retry state are owned exclusively by the worker.
   */
  uv_loop_t worker_loop;
  uv_async_t work_async;
  uv_timer_t retry_timer;
  uv_thread_t worker_thread;
  uv_mutex_t queue_mutex;
  uv_cond_t startup_condition;
  bool loop_initialized;
  bool work_async_initialized;
  bool retry_timer_initialized;
  bool mutex_initialized;
  bool condition_initialized;
  bool worker_started;
  bool worker_ready;
  bool worker_start_ok;
  bool stopping;

  /* Preallocated descriptor ring; individual payload copies remain owned. */
  ws_rabbitmq_replay_message_t *replay_queue;
  size_t replay_count;
  size_t replay_bytes;
  size_t replay_capacity;
  size_t replay_head;
  uint64_t retry_not_before_ms;
  uint32_t retry_backoff_ms;
  uint32_t skipped_flush_attempts;
  bool replay_queue_full_logged;
  uint64_t replay_queue_dropped_messages;
};

/* Connection functions do not own the publisher passed to them. */
[[nodiscard]] bool
ws_rabbitmq_connection_open(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_connection_close(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] bool ws_rabbitmq_connection_publish_batch(
    ws_rabbitmq_publisher_t *publisher,
    const ws_rabbitmq_replay_message_t *const *messages, size_t message_count);

/* The worker owns the libuv loop and all broker operations. */
[[nodiscard]] bool ws_rabbitmq_worker_start(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_worker_stop(ws_rabbitmq_publisher_t *publisher);

/* Retry and replay functions mutate only the publisher's bounded queue. */
[[nodiscard]] bool
ws_rabbitmq_retry_allows_flush(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_retry_record_failure(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_retry_record_success(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] uint64_t
ws_rabbitmq_retry_delay_ms(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] bool
ws_rabbitmq_replay_initialize(ws_rabbitmq_publisher_t *publisher);
void ws_rabbitmq_replay_clear(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] bool
ws_rabbitmq_replay_enqueue(ws_rabbitmq_publisher_t *publisher,
                           const char *payload, size_t payload_length);
[[nodiscard]] size_t
ws_rabbitmq_replay_peek_batch(ws_rabbitmq_publisher_t *publisher,
                              const ws_rabbitmq_replay_message_t **messages,
                              size_t message_capacity);
void ws_rabbitmq_replay_drop_head(ws_rabbitmq_publisher_t *publisher,
                                  size_t message_count);
[[nodiscard]] size_t
ws_rabbitmq_replay_count(ws_rabbitmq_publisher_t *publisher);
