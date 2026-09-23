#define _POSIX_C_SOURCE 200809L
#include "rabbitmq_tx_console_internal.h"
#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "ulog/ulog.h"
#include <sys/time.h>
#include <time.h>

/* Broker replies, envelopes, and acknowledgement results are untrusted. */

static void app_log_rpc_failure(const char *action, amqp_rpc_reply_t reply) {
  if (action == nullptr) {
    action = "operation";
  }

  if (reply.reply_type == AMQP_RESPONSE_NONE) {
    ulog_error("RabbitMQ %s failed: missing RPC reply\n", action);
    return;
  }
  if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: %s\n", action,
               amqp_error_string2(reply.library_error));
    return;
  }
  if (reply.reply_type != AMQP_RESPONSE_SERVER_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: unexpected reply type=%d\n", action,
               reply.reply_type);
    return;
  }

  if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
    amqp_connection_close_t *connection_close =
        (amqp_connection_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server connection close %u (%.*s)\n",
               action, connection_close->reply_code,
               (int)connection_close->reply_text.len,
               (char *)connection_close->reply_text.bytes);
    return;
  }

  if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
    amqp_channel_close_t *channel_close =
        (amqp_channel_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server channel close %u (%.*s)\n", action,
               channel_close->reply_code, (int)channel_close->reply_text.len,
               (char *)channel_close->reply_text.bytes);
    return;
  }

  ulog_error("RabbitMQ %s failed: server exception method=0x%08X\n", action,
             reply.reply.id);
}

[[nodiscard]] static bool
app_expect_normal_reply(amqp_connection_state_t connection,
                        const char *action) {
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection);
  if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
    return true;
  }

  app_log_rpc_failure(action, reply);
  return false;
}

void app_rabbitmq_disconnect(app_rabbitmq_consumer_t *consumer) {
  if (consumer == nullptr || consumer->connection == nullptr) {
    return;
  }

  if (consumer->channel_open) {
    (void)amqp_channel_close(consumer->connection, consumer->channel,
                             AMQP_REPLY_SUCCESS);
    consumer->channel_open = false;
  }

  if (consumer->logged_in) {
    (void)amqp_connection_close(consumer->connection, AMQP_REPLY_SUCCESS);
    consumer->logged_in = false;
  }

  int destroy_status = amqp_destroy_connection(consumer->connection);
  if (destroy_status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ destroy_connection returned: %s\n",
               amqp_error_string2(destroy_status));
  }

  consumer->connection = nullptr;
}

[[nodiscard]] bool app_rabbitmq_connect(const app_config_t *config,
                                        app_rabbitmq_consumer_t *consumer) {
  if (config == nullptr || consumer == nullptr ||
      config->rabbitmq_host == nullptr || config->rabbitmq_port == 0 ||
      config->rabbitmq_username == nullptr ||
      config->rabbitmq_password == nullptr ||
      config->rabbitmq_vhost == nullptr || config->rabbitmq_queue == nullptr ||
      config->rabbitmq_channel == 0 ||
      config->rabbitmq_heartbeat_seconds == 0) {
    ulog_error("Invalid RabbitMQ configuration\n");
    return false;
  }

  *consumer = (app_rabbitmq_consumer_t){
      .channel = (amqp_channel_t)config->rabbitmq_channel,
  };

  consumer->connection = amqp_new_connection();
  if (consumer->connection == nullptr) {
    ulog_error("Failed to create RabbitMQ connection object\n");
    return false;
  }

  amqp_socket_t *socket = amqp_tcp_socket_new(consumer->connection);
  if (socket == nullptr) {
    ulog_error("Failed to create RabbitMQ TCP socket\n");
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  int socket_status = amqp_socket_open(socket, config->rabbitmq_host,
                                       (int)config->rabbitmq_port);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s\n",
               config->rabbitmq_host, config->rabbitmq_port,
               amqp_error_string2(socket_status));
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  amqp_rpc_reply_t login_reply = amqp_login(
      consumer->connection, config->rabbitmq_vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      (int)config->rabbitmq_heartbeat_seconds, AMQP_SASL_METHOD_PLAIN,
      config->rabbitmq_username, config->rabbitmq_password);
  if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    app_log_rpc_failure("login", login_reply);
    app_rabbitmq_disconnect(consumer);
    return false;
  }
  consumer->logged_in = true;

  if (amqp_channel_open(consumer->connection, consumer->channel) == nullptr ||
      !app_expect_normal_reply(consumer->connection, "channel open")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }
  consumer->channel_open = true;

  amqp_bytes_t queue_bytes = amqp_cstring_bytes(config->rabbitmq_queue);
  amqp_queue_declare_ok_t *queue_declare_ok = amqp_queue_declare(
      consumer->connection, consumer->channel, queue_bytes, false,
      config->rabbitmq_queue_durable, false, false, amqp_empty_table);
  if (queue_declare_ok == nullptr ||
      !app_expect_normal_reply(consumer->connection, "queue declare")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  if (config->prefetch_count > 0) {
    amqp_basic_qos_ok_t *qos_ok =
        amqp_basic_qos(consumer->connection, consumer->channel, 0,
                       config->prefetch_count, false);
    if (qos_ok == nullptr ||
        !app_expect_normal_reply(consumer->connection, "basic.qos")) {
      app_rabbitmq_disconnect(consumer);
      return false;
    }
  }

  amqp_basic_consume_ok_t *consume_ok = amqp_basic_consume(
      consumer->connection, consumer->channel, queue_bytes, amqp_empty_bytes,
      false, config->auto_ack, false, amqp_empty_table);
  if (consume_ok == nullptr ||
      !app_expect_normal_reply(consumer->connection, "basic.consume")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  ulog_info(
      "Connected to RabbitMQ at %s:%u (queue=%s durable=%s prefetch=%u "
      "auto_ack=%s)",
      config->rabbitmq_host, config->rabbitmq_port, config->rabbitmq_queue,
      config->rabbitmq_queue_durable ? "true" : "false",
      (unsigned)config->prefetch_count, config->auto_ack ? "true" : "false");

  return true;
}

[[nodiscard]] bool app_consume_loop(app_rabbitmq_consumer_t *consumer,
                                    const app_config_t *config) {
  if (consumer == nullptr || config == nullptr ||
      consumer->connection == nullptr) {
    return false;
  }

  struct timeval timeout = {
      .tv_sec = (time_t)config->read_timeout_seconds,
      .tv_usec = 0,
  };

  while (!app_is_shutdown_requested()) {
    amqp_maybe_release_buffers(consumer->connection);

    amqp_envelope_t envelope = {0};
    amqp_rpc_reply_t reply =
        amqp_consume_message(consumer->connection, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      app_handle_payload(envelope.message.body.bytes,
                         envelope.message.body.len);

      if (!config->auto_ack) {
        int ack_status = amqp_basic_ack(consumer->connection, consumer->channel,
                                        envelope.delivery_tag, false);
        if (ack_status != AMQP_STATUS_OK) {
          ulog_error("RabbitMQ ack failed: %s\n",
                     amqp_error_string2(ack_status));
          amqp_destroy_envelope(&envelope);
          return false;
        }
      }

      amqp_destroy_envelope(&envelope);
      continue;
    }

    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      continue;
    }

    app_log_rpc_failure("consume message", reply);
    return false;
  }

  return true;
}
