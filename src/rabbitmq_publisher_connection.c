#include "rabbitmq_publisher_internal.h"

#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "ulog/ulog.h"

#include <sys/time.h>

constexpr uint32_t WS_RABBITMQ_PUBLISH_CONFIRM_TIMEOUT_MS = 3'000;

[[nodiscard]] static bool
ws_rabbitmq_wait_for_publish_confirm(ws_rabbitmq_publisher_t *publisher);

/*
 * Broker trust boundary:
 * AMQP replies and publisher confirms originate from the remote broker. This
 * module validates reply classes, methods, and channels before accepting them.
 */

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

void ws_rabbitmq_connection_close(ws_rabbitmq_publisher_t *publisher) {
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

[[nodiscard]] bool
ws_rabbitmq_connection_publish(ws_rabbitmq_publisher_t *publisher,
                               const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return false;
  }

  if (publisher->connection == nullptr &&
      !ws_rabbitmq_connection_open(publisher)) {
    return false;
  }

  auto publish_status = amqp_basic_publish(
      publisher->connection, publisher->channel, amqp_empty_bytes,
      publisher->queue_bytes, false, false, &publisher->publish_properties,
      amqp_bytes_from_buffer(payload, payload_length));
  if (publish_status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ publish failed: %s",
               amqp_error_string2(publish_status));
    ws_rabbitmq_connection_close(publisher);
    return false;
  }

  if (!ws_rabbitmq_wait_for_publish_confirm(publisher)) {
    ws_rabbitmq_connection_close(publisher);
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

[[nodiscard]] bool
ws_rabbitmq_connection_open(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  ws_rabbitmq_connection_close(publisher);
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
    ws_rabbitmq_connection_close(publisher);
    return false;
  }

  auto socket_status =
      amqp_socket_open(socket, publisher->host, (int)publisher->port);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s", publisher->host,
               publisher->port, amqp_error_string2(socket_status));
    ws_rabbitmq_connection_close(publisher);
    return false;
  }

  auto login_reply = amqp_login(
      publisher->connection, publisher->vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      publisher->heartbeat_seconds, AMQP_SASL_METHOD_PLAIN, publisher->username,
      publisher->password);
  if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    ws_rabbitmq_log_rpc_failure("login", login_reply);
    ws_rabbitmq_connection_close(publisher);
    return false;
  }
  publisher->logged_in = true;

  if (amqp_channel_open(publisher->connection, publisher->channel) == nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection, "channel open")) {
    ws_rabbitmq_connection_close(publisher);
    return false;
  }
  publisher->channel_open = true;

  amqp_queue_declare_ok_t *queue_declare_ok = amqp_queue_declare(
      publisher->connection, publisher->channel, publisher->queue_bytes, false,
      publisher->queue_durable, false, false, amqp_empty_table);
  if (queue_declare_ok == nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection,
                                       "queue declare")) {
    ws_rabbitmq_connection_close(publisher);
    return false;
  }

  if (amqp_confirm_select(publisher->connection, publisher->channel) ==
          nullptr ||
      !ws_rabbitmq_expect_normal_reply(publisher->connection,
                                       "confirm select")) {
    ws_rabbitmq_connection_close(publisher);
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
