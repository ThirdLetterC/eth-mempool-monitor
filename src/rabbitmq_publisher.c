#include "websocket-client/rabbitmq_publisher.h"

#include "rabbitmq/amqp.h"
#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "ulog/ulog.h"

#include <inttypes.h>
#include <stdckdint.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

constexpr amqp_channel_t WS_RABBITMQ_DEFAULT_CHANNEL = 1;
constexpr uint16_t WS_RABBITMQ_DEFAULT_HEARTBEAT_SECONDS = 30;
constexpr char WS_RABBITMQ_CONTENT_TYPE[] = "application/json";
constexpr size_t WS_RABBITMQ_MAX_REPLAY_MESSAGES = 4'096;
constexpr uint32_t WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS = 3;
constexpr uint32_t WS_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS = 3'000;
constexpr uint32_t WS_RABBITMQ_RETRY_INITIAL_BACKOFF_MS = 1'000;
constexpr uint32_t WS_RABBITMQ_RETRY_MAX_BACKOFF_MS = 60'000;

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

[[nodiscard]] static bool
ws_rabbitmq_wait_for_publish_confirm(ws_rabbitmq_publisher_t *publisher);
[[nodiscard]] static bool
ws_rabbitmq_connect(ws_rabbitmq_publisher_t *publisher);

static void ws_rabbitmq_log_rpc_failure(const char *action,
                                        amqp_rpc_reply_t reply) {
  if (reply.reply_type == AMQP_RESPONSE_NONE) {
    ulog_error("RabbitMQ %s failed: missing RPC reply", action);
    return;
  }
  if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: %s", action,
               amqp_error_string2(reply.library_error));
    return;
  }
  if (reply.reply_type != AMQP_RESPONSE_SERVER_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: unexpected reply type=%d", action,
               reply.reply_type);
    return;
  }

  if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
    amqp_connection_close_t *connection_close =
        (amqp_connection_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server connection close %u (%.*s)", action,
               connection_close->reply_code,
               (int)connection_close->reply_text.len,
               (char *)connection_close->reply_text.bytes);
    return;
  }
  if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
    amqp_channel_close_t *channel_close =
        (amqp_channel_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server channel close %u (%.*s)", action,
               channel_close->reply_code, (int)channel_close->reply_text.len,
               (char *)channel_close->reply_text.bytes);
    return;
  }

  ulog_error("RabbitMQ %s failed: server exception method=0x%08X", action,
             reply.reply.id);
}

[[nodiscard]] static uint64_t ws_rabbitmq_now_milliseconds() {
  struct timeval tv = {0};
  if (gettimeofday(&tv, nullptr) == 0) {
    return (uint64_t)tv.tv_sec * 1000U + (uint64_t)tv.tv_usec / 1000U;
  }

  time_t now = time(nullptr);
  if (now < 0) {
    return 0;
  }
  return (uint64_t)now * 1000U;
}

[[nodiscard]] static bool
ws_rabbitmq_retry_allows_flush(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  uint64_t now_ms = ws_rabbitmq_now_milliseconds();
  if (publisher->retry_not_before_ms != 0 &&
      now_ms < publisher->retry_not_before_ms) {
    if (publisher->skipped_flush_attempts < UINT32_MAX) {
      publisher->skipped_flush_attempts += 1;
    }
    return false;
  }

  if (publisher->skipped_flush_attempts > 0) {
    ulog_info(
        "RabbitMQ publish retrying after cooldown (queued=%zu skipped=%u)",
        publisher->replay_count, (unsigned)publisher->skipped_flush_attempts);
    publisher->skipped_flush_attempts = 0;
  }

  return true;
}

static void
ws_rabbitmq_retry_record_failure(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }

  if (publisher->retry_backoff_ms == 0) {
    publisher->retry_backoff_ms = WS_RABBITMQ_RETRY_INITIAL_BACKOFF_MS;
  } else if (publisher->retry_backoff_ms < WS_RABBITMQ_RETRY_MAX_BACKOFF_MS) {
    uint64_t doubled_backoff = (uint64_t)publisher->retry_backoff_ms * 2U;
    publisher->retry_backoff_ms =
        doubled_backoff > WS_RABBITMQ_RETRY_MAX_BACKOFF_MS
            ? WS_RABBITMQ_RETRY_MAX_BACKOFF_MS
            : (uint32_t)doubled_backoff;
  }

  publisher->retry_not_before_ms =
      ws_rabbitmq_now_milliseconds() + publisher->retry_backoff_ms;
  ulog_warn("RabbitMQ unavailable, backing off publish attempts for %u ms "
            "(queued=%zu)",
            (unsigned)publisher->retry_backoff_ms, publisher->replay_count);
}

static void
ws_rabbitmq_retry_record_success(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }

  if (publisher->retry_backoff_ms > 0) {
    ulog_info("RabbitMQ publish path recovered (queued=%zu)",
              publisher->replay_count);
  }
  publisher->retry_not_before_ms = 0;
  publisher->retry_backoff_ms = 0;
  publisher->skipped_flush_attempts = 0;
}

[[nodiscard]] static bool
ws_rabbitmq_expect_normal_reply(amqp_connection_state_t connection,
                                const char *action) {
  auto reply = amqp_get_rpc_reply(connection);
  if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
    return true;
  }
  ws_rabbitmq_log_rpc_failure(action, reply);
  return false;
}

static void ws_rabbitmq_disconnect(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->connection == nullptr) {
    return;
  }

  if (publisher->channel_open) {
    (void)amqp_channel_close(publisher->connection, publisher->channel,
                             AMQP_REPLY_SUCCESS);
    publisher->channel_open = false;
  }

  if (publisher->logged_in) {
    (void)amqp_connection_close(publisher->connection, AMQP_REPLY_SUCCESS);
    publisher->logged_in = false;
  }

  auto destroy_status = amqp_destroy_connection(publisher->connection);
  if (destroy_status != AMQP_STATUS_OK) {
    ulog_warn("RabbitMQ destroy_connection returned: %s",
              amqp_error_string2(destroy_status));
  }
  publisher->connection = nullptr;
}

[[nodiscard]] static char *ws_rabbitmq_string_duplicate(const char *text) {
  if (text == nullptr) {
    return nullptr;
  }

  size_t len = strlen(text);
  size_t allocation_length = 0;
  if (ckd_add(&allocation_length, len, (size_t)1)) {
    return nullptr;
  }

  char *copy = calloc(allocation_length, sizeof(char));
  if (copy == nullptr) {
    return nullptr;
  }
  memcpy(copy, text, len + 1);
  return copy;
}

static void
ws_rabbitmq_cleanup_owned_fields(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  free(publisher->host_owned);
  free(publisher->username_owned);
  free(publisher->password_owned);
  free(publisher->vhost_owned);
  free(publisher->queue_owned);
  publisher->host_owned = nullptr;
  publisher->username_owned = nullptr;
  publisher->password_owned = nullptr;
  publisher->vhost_owned = nullptr;
  publisher->queue_owned = nullptr;
  publisher->host = nullptr;
  publisher->username = nullptr;
  publisher->password = nullptr;
  publisher->vhost = nullptr;
  publisher->queue = nullptr;
}

static void ws_rabbitmq_clear_replay_queue(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_queue == nullptr) {
    return;
  }
  if (publisher->replay_capacity == 0) {
    free(publisher->replay_queue);
    publisher->replay_queue = nullptr;
    publisher->replay_count = 0;
    publisher->replay_head = 0;
    return;
  }
  for (size_t i = 0; i < publisher->replay_count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    free(publisher->replay_queue[index].payload);
  }
  free(publisher->replay_queue);
  publisher->replay_queue = nullptr;
  publisher->replay_count = 0;
  publisher->replay_capacity = 0;
  publisher->replay_head = 0;
}

[[nodiscard]] static bool
ws_rabbitmq_reserve_replay_capacity(ws_rabbitmq_publisher_t *publisher,
                                    size_t new_capacity) {
  if (publisher == nullptr) {
    return false;
  }
  if (new_capacity <= publisher->replay_capacity) {
    return true;
  }
  if (new_capacity > SIZE_MAX / sizeof(*publisher->replay_queue)) {
    return false;
  }

  auto resized = (ws_rabbitmq_replay_message_t *)calloc(
      new_capacity, sizeof(*publisher->replay_queue));
  if (resized == nullptr) {
    return false;
  }

  for (size_t i = 0; i < publisher->replay_count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    resized[i] = publisher->replay_queue[index];
  }
  free(publisher->replay_queue);

  publisher->replay_queue = resized;
  publisher->replay_capacity = new_capacity;
  publisher->replay_head = 0;
  return true;
}

[[nodiscard]] static ws_rabbitmq_replay_message_t *
ws_rabbitmq_replay_head_message(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_count == 0 ||
      publisher->replay_queue == nullptr ||
      publisher->replay_head >= publisher->replay_capacity) {
    return nullptr;
  }
  return &publisher->replay_queue[publisher->replay_head];
}

[[nodiscard]] static bool
ws_rabbitmq_enqueue_replay_message(ws_rabbitmq_publisher_t *publisher,
                                   const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return false;
  }

  if (publisher->replay_count >= WS_RABBITMQ_MAX_REPLAY_MESSAGES) {
    if (publisher->replay_queue_dropped_messages < UINT64_MAX) {
      publisher->replay_queue_dropped_messages += 1;
    }
    if (!publisher->replay_queue_full_logged) {
      ulog_error(
          "RabbitMQ replay queue is full (%zu messages), dropping publishes "
          "until broker recovers",
          publisher->replay_count);
      publisher->replay_queue_full_logged = true;
    }
    return false;
  }

  size_t payload_capacity = 0;
  if (ckd_add(&payload_capacity, payload_length, (size_t)1)) {
    return false;
  }

  char *payload_copy = calloc(payload_capacity, sizeof(char));
  if (payload_copy == nullptr) {
    ulog_error("Failed to allocate replay payload copy");
    return false;
  }
  if (payload_length > 0) {
    memcpy(payload_copy, payload, payload_length);
  }

  if (publisher->replay_count == publisher->replay_capacity) {
    size_t new_capacity =
        publisher->replay_capacity == 0 ? 32 : publisher->replay_capacity * 2;
    if (new_capacity > WS_RABBITMQ_MAX_REPLAY_MESSAGES) {
      new_capacity = WS_RABBITMQ_MAX_REPLAY_MESSAGES;
    }
    if (new_capacity < publisher->replay_count) {
      free(payload_copy);
      return false;
    }

    if (!ws_rabbitmq_reserve_replay_capacity(publisher, new_capacity)) {
      free(payload_copy);
      return false;
    }
  }

  if (publisher->replay_queue_full_logged) {
    ulog_warn("RabbitMQ replay queue accepts publishes again (dropped=%" PRIu64
              ")",
              publisher->replay_queue_dropped_messages);
    publisher->replay_queue_full_logged = false;
    publisher->replay_queue_dropped_messages = 0;
  }

  size_t index = (publisher->replay_head + publisher->replay_count) %
                 publisher->replay_capacity;
  publisher->replay_queue[index] = (ws_rabbitmq_replay_message_t){
      .payload = payload_copy, .payload_length = payload_length};
  publisher->replay_count += 1;
  return true;
}

static void ws_rabbitmq_drop_replay_head(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_count == 0 ||
      publisher->replay_queue == nullptr) {
    return;
  }

  free(publisher->replay_queue[publisher->replay_head].payload);
  publisher->replay_queue[publisher->replay_head] =
      (ws_rabbitmq_replay_message_t){0};
  publisher->replay_head =
      (publisher->replay_head + 1) % publisher->replay_capacity;
  publisher->replay_count -= 1;

  if (publisher->replay_count == 0) {
    free(publisher->replay_queue);
    publisher->replay_queue = nullptr;
    publisher->replay_capacity = 0;
    publisher->replay_head = 0;
  }
}

[[nodiscard]] static bool
ws_rabbitmq_publish_single(ws_rabbitmq_publisher_t *publisher,
                           const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return false;
  }

  if (publisher->connection == nullptr && !ws_rabbitmq_connect(publisher)) {
    return false;
  }

  auto publish_status = amqp_basic_publish(
      publisher->connection, publisher->channel, amqp_empty_bytes,
      publisher->queue_bytes, false, false, &publisher->publish_properties,
      amqp_bytes_from_buffer(payload, payload_length));
  if (publish_status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ publish failed: %s",
               amqp_error_string2(publish_status));
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  if (!ws_rabbitmq_wait_for_publish_confirm(publisher)) {
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  return true;
}

[[nodiscard]] static bool
ws_rabbitmq_wait_for_publish_confirm(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->connection == nullptr) {
    return false;
  }

  struct timeval timeout = {
      .tv_sec = WS_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS / 1000U,
      .tv_usec =
          (suseconds_t)((WS_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS % 1000U) *
                        1000U),
  };

  amqp_publisher_confirm_t confirm = {0};
  amqp_rpc_reply_t reply =
      amqp_publisher_confirm_wait(publisher->connection, &timeout, &confirm);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    ws_rabbitmq_log_rpc_failure("publisher confirm wait", reply);
    return false;
  }

  if (confirm.channel != publisher->channel) {
    ulog_error("RabbitMQ publish confirm arrived on unexpected channel=%u",
               (unsigned)confirm.channel);
    return false;
  }

  if (confirm.method == AMQP_BASIC_ACK_METHOD) {
    return true;
  }
  if (confirm.method == AMQP_BASIC_NACK_METHOD) {
    ulog_error("RabbitMQ publish was nacked by broker");
    return false;
  }
  if (confirm.method == AMQP_BASIC_REJECT_METHOD) {
    ulog_error("RabbitMQ publish was rejected by broker");
    return false;
  }

  ulog_error("RabbitMQ publish confirm returned unexpected method=0x%08X",
             confirm.method);
  return false;
}

[[nodiscard]] static bool
ws_rabbitmq_connect(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  ws_rabbitmq_disconnect(publisher);
  publisher->logged_in = false;
  publisher->channel_open = false;

  publisher->connection = amqp_new_connection();
  if (publisher->connection == nullptr) {
    ulog_error("Failed to create RabbitMQ connection object");
    return false;
  }

  amqp_socket_t *socket = amqp_tcp_socket_new(publisher->connection);
  if (socket == nullptr) {
    ulog_error("Failed to create RabbitMQ TCP socket");
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  auto socket_status =
      amqp_socket_open(socket, publisher->host, (int)publisher->port);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s", publisher->host,
               publisher->port, amqp_error_string2(socket_status));
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  auto login_reply = amqp_login(
      publisher->connection, publisher->vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      publisher->heartbeat_seconds, AMQP_SASL_METHOD_PLAIN, publisher->username,
      publisher->password);
  if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    ws_rabbitmq_log_rpc_failure("login", login_reply);
    ws_rabbitmq_disconnect(publisher);
    return false;
  }
  publisher->logged_in = true;

  if (amqp_channel_open(publisher->connection, publisher->channel) == nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection, "channel open")) {
    ws_rabbitmq_disconnect(publisher);
    return false;
  }
  publisher->channel_open = true;

  amqp_queue_declare_ok_t *queue_declare_ok = amqp_queue_declare(
      publisher->connection, publisher->channel, publisher->queue_bytes, false,
      publisher->queue_durable, false, false, amqp_empty_table);
  if (queue_declare_ok == nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection,
                                       "queue declare")) {
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  if (amqp_confirm_select(publisher->connection, publisher->channel) ==
          nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection,
                                       "confirm select")) {
    ws_rabbitmq_disconnect(publisher);
    return false;
  }

  ulog_info("Connected to RabbitMQ at %s:%u (queue=%s durable=%s channel=%u "
            "heartbeat=%u confirms=on)",
            publisher->host, publisher->port, publisher->queue,
            publisher->queue_durable ? "true" : "false",
            (unsigned)publisher->channel,
            (unsigned)publisher->heartbeat_seconds);
  return true;
}

[[nodiscard]] static bool
ws_rabbitmq_flush_replay_queue(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  while (publisher->replay_count > 0) {
    ws_rabbitmq_replay_message_t *message =
        ws_rabbitmq_replay_head_message(publisher);
    if (message == nullptr) {
      return false;
    }
    if (!ws_rabbitmq_publish_single(publisher, message->payload,
                                    message->payload_length)) {
      return false;
    }

    ws_rabbitmq_drop_replay_head(publisher);
  }

  return true;
}

[[nodiscard]] ws_rabbitmq_publisher_t *
ws_rabbitmq_publisher_create(const ws_rabbitmq_config_t *config) {
  if (config == nullptr || config->host == nullptr || config->port == 0 ||
      config->username == nullptr || config->password == nullptr ||
      config->vhost == nullptr || config->queue == nullptr) {
    ulog_error("Invalid RabbitMQ configuration");
    return nullptr;
  }

  ws_rabbitmq_publisher_t *publisher =
      calloc(1, sizeof(ws_rabbitmq_publisher_t));
  if (publisher == nullptr) {
    ulog_error("Failed to allocate RabbitMQ publisher");
    return nullptr;
  }

  publisher->host_owned = ws_rabbitmq_string_duplicate(config->host);
  publisher->username_owned = ws_rabbitmq_string_duplicate(config->username);
  publisher->password_owned = ws_rabbitmq_string_duplicate(config->password);
  publisher->vhost_owned = ws_rabbitmq_string_duplicate(config->vhost);
  publisher->queue_owned = ws_rabbitmq_string_duplicate(config->queue);
  if (publisher->host_owned == nullptr ||
      publisher->username_owned == nullptr ||
      publisher->password_owned == nullptr ||
      publisher->vhost_owned == nullptr || publisher->queue_owned == nullptr) {
    ulog_error("Out of memory while copying RabbitMQ configuration");
    ws_rabbitmq_cleanup_owned_fields(publisher);
    free(publisher);
    return nullptr;
  }

  publisher->host = publisher->host_owned;
  publisher->username = publisher->username_owned;
  publisher->password = publisher->password_owned;
  publisher->vhost = publisher->vhost_owned;
  publisher->queue = publisher->queue_owned;
  publisher->port = config->port;
  publisher->queue_durable = config->queue_durable;
  publisher->channel = config->channel != 0
                           ? config->channel
                           : (uint16_t)WS_RABBITMQ_DEFAULT_CHANNEL;
  publisher->heartbeat_seconds = config->heartbeat_seconds != 0
                                     ? config->heartbeat_seconds
                                     : WS_RABBITMQ_DEFAULT_HEARTBEAT_SECONDS;
  publisher->queue_bytes = amqp_cstring_bytes(publisher->queue);
  publisher->publish_properties = (amqp_basic_properties_t){0};
  publisher->publish_properties._flags =
      AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  publisher->publish_properties.content_type =
      amqp_cstring_bytes(WS_RABBITMQ_CONTENT_TYPE);
  publisher->publish_properties.delivery_mode =
      publisher->queue_durable ? AMQP_DELIVERY_PERSISTENT
                               : AMQP_DELIVERY_NONPERSISTENT;

  if (!ws_rabbitmq_connect(publisher)) {
    ws_rabbitmq_clear_replay_queue(publisher);
    ws_rabbitmq_cleanup_owned_fields(publisher);
    free(publisher);
    return nullptr;
  }

  return publisher;
}

[[nodiscard]] bool
ws_rabbitmq_publisher_publish(ws_rabbitmq_publisher_t *publisher,
                              const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return false;
  }

  if (publisher->replay_count == 0 &&
      ws_rabbitmq_retry_allows_flush(publisher)) {
    for (uint32_t attempt = 0; attempt < WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS;
         ++attempt) {
      if (ws_rabbitmq_publish_single(publisher, payload, payload_length)) {
        ws_rabbitmq_retry_record_success(publisher);
        return true;
      }

      if (attempt + 1 < WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS) {
        ulog_warn("RabbitMQ direct publish failed, retrying (attempt=%u/%u)",
                  (unsigned)(attempt + 1),
                  (unsigned)WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS);
      }
    }

    if (!ws_rabbitmq_enqueue_replay_message(publisher, payload,
                                            payload_length) &&
        publisher->replay_count == 0) {
      ws_rabbitmq_retry_record_failure(publisher);
      return false;
    }

    ulog_error(
        "RabbitMQ publish failed after %u attempts; replay queue depth=%zu",
        (unsigned)WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS, publisher->replay_count);
    ws_rabbitmq_retry_record_failure(publisher);
    return false;
  }

  bool enqueued =
      ws_rabbitmq_enqueue_replay_message(publisher, payload, payload_length);
  if (!enqueued && publisher->replay_count == 0) {
    ws_rabbitmq_retry_record_failure(publisher);
    return false;
  }

  if (!ws_rabbitmq_retry_allows_flush(publisher)) {
    return false;
  }

  for (uint32_t attempt = 0; attempt < WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS;
       ++attempt) {
    if (ws_rabbitmq_flush_replay_queue(publisher)) {
      ws_rabbitmq_retry_record_success(publisher);
      return true;
    }

    if (attempt + 1 < WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS) {
      ulog_warn("RabbitMQ publish replay pending (%zu message%s), retrying "
                "(attempt=%u/%u)",
                publisher->replay_count,
                publisher->replay_count == 1 ? "" : "s",
                (unsigned)(attempt + 1),
                (unsigned)WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS);
    }
  }

  ulog_error(
      "RabbitMQ publish failed after %u attempts; replay queue depth=%zu",
      (unsigned)WS_RABBITMQ_PUBLISH_RETRY_ATTEMPTS, publisher->replay_count);
  ws_rabbitmq_retry_record_failure(publisher);
  return false;
}

void ws_rabbitmq_publisher_destroy(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  ws_rabbitmq_disconnect(publisher);
  ws_rabbitmq_clear_replay_queue(publisher);
  ws_rabbitmq_cleanup_owned_fields(publisher);
  free(publisher);
}
