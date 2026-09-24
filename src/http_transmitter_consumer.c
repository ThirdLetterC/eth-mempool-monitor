#include "websocket-client/http_transmitter_internal.h"
#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "ulog/ulog.h"

#include <sys/time.h>
#include <time.h>

static void http_log_rpc_failure(const char *action, amqp_rpc_reply_t reply) {
  if (reply.reply_type == AMQP_RESPONSE_NONE) {
    ulog_error("RabbitMQ %s failed: missing RPC reply", action);
  } else if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: %s", action,
               amqp_error_string2(reply.library_error));
  } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
             reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
    const amqp_connection_close_t *close =
        (const amqp_connection_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: connection close %u (%.*s)", action,
               close->reply_code, (int)close->reply_text.len,
               (const char *)close->reply_text.bytes);
  } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
             reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
    const amqp_channel_close_t *close =
        (const amqp_channel_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: channel close %u (%.*s)", action,
               close->reply_code, (int)close->reply_text.len,
               (const char *)close->reply_text.bytes);
  } else {
    ulog_error("RabbitMQ %s failed: unexpected reply type=%d", action,
               reply.reply_type);
  }
}

[[nodiscard]] static bool
http_expect_normal_reply(amqp_connection_state_t connection,
                         const char *action) {
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection);
  if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
    return true;
  }
  http_log_rpc_failure(action, reply);
  return false;
}

void http_transmitter_rabbitmq_disconnect(
    http_transmitter_consumer_t *consumer) {
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
  int status = amqp_destroy_connection(consumer->connection);
  if (status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ destroy_connection returned: %s",
               amqp_error_string2(status));
  }
  consumer->connection = nullptr;
}

[[nodiscard]] http_transmitter_status_t
http_transmitter_rabbitmq_connect(const http_transmitter_config_t *config,
                                  http_transmitter_consumer_t *consumer) {
  if (config == nullptr || consumer == nullptr ||
      config->rabbitmq_host == nullptr || config->rabbitmq_port.value == 0 ||
      config->rabbitmq_username == nullptr ||
      config->rabbitmq_password == nullptr ||
      config->rabbitmq_vhost == nullptr || config->rabbitmq_queue == nullptr ||
      config->rabbitmq_channel.value == 0 ||
      config->rabbitmq_heartbeat.value == 0 ||
      config->prefetch_count.value == 0) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  *consumer = (http_transmitter_consumer_t){
      .channel = (amqp_channel_t)config->rabbitmq_channel.value,
  };
  consumer->connection = amqp_new_connection();
  if (consumer->connection == nullptr) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  amqp_socket_t *socket = amqp_tcp_socket_new(consumer->connection);
  if (socket == nullptr) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  int socket_status = amqp_socket_open(socket, config->rabbitmq_host,
                                       (int)config->rabbitmq_port.value);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s",
               config->rabbitmq_host, (unsigned)config->rabbitmq_port.value,
               amqp_error_string2(socket_status));
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_CONNECTION_ERROR;
  }
  amqp_rpc_reply_t login = amqp_login(
      consumer->connection, config->rabbitmq_vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      (int)config->rabbitmq_heartbeat.value, AMQP_SASL_METHOD_PLAIN,
      config->rabbitmq_username, config->rabbitmq_password);
  if (login.reply_type != AMQP_RESPONSE_NORMAL) {
    http_log_rpc_failure("login", login);
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_CONNECTION_ERROR;
  }
  consumer->logged_in = true;
  if (amqp_channel_open(consumer->connection, consumer->channel) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "channel open")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  consumer->channel_open = true;

  amqp_bytes_t queue = amqp_cstring_bytes(config->rabbitmq_queue);
  if (amqp_queue_declare(consumer->connection, consumer->channel, queue, false,
                         config->rabbitmq_queue_durable, false, false,
                         amqp_empty_table) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "queue declare")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  if (amqp_basic_qos(consumer->connection, consumer->channel, 0,
                     config->prefetch_count.value, false) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "basic.qos")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  if (amqp_basic_consume(consumer->connection, consumer->channel, queue,
                         amqp_empty_bytes, false, false, false,
                         amqp_empty_table) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "basic.consume")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  ulog_info("Connected to RabbitMQ at %s:%u (queue=%s, prefetch=%u)",
            config->rabbitmq_host, (unsigned)config->rabbitmq_port.value,
            config->rabbitmq_queue, (unsigned)config->prefetch_count.value);
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] static http_transmitter_status_t
http_settle_delivery(http_transmitter_consumer_t *consumer,
                     uint64_t delivery_tag, http_delivery_result_t result) {
  int status = AMQP_STATUS_OK;
  if (result == HTTP_DELIVERY_SUCCESS) {
    status = amqp_basic_ack(consumer->connection, consumer->channel,
                            delivery_tag, false);
  } else if (result == HTTP_DELIVERY_PERMANENT_FAILURE) {
    status = amqp_basic_reject(consumer->connection, consumer->channel,
                               delivery_tag, false);
  } else {
    status = amqp_basic_nack(consumer->connection, consumer->channel,
                             delivery_tag, false, true);
  }
  if (status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ delivery settlement failed: %s",
               amqp_error_string2(status));
    return HTTP_TRANSMITTER_STATUS_ACK_ERROR;
  }
  if (result == HTTP_DELIVERY_TRANSIENT_FAILURE) {
    ulog_warn("Webhook retries exhausted; RabbitMQ delivery requeued");
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] http_transmitter_status_t
http_transmitter_consume_loop(http_transmitter_consumer_t *consumer,
                              http_webhook_client_t *webhook,
                              const http_transmitter_config_t *config) {
  if (consumer == nullptr || webhook == nullptr || config == nullptr ||
      consumer->connection == nullptr) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  struct timeval timeout = {
      .tv_sec = (time_t)config->read_timeout.value,
      .tv_usec = 0,
  };
  while (!http_transmitter_is_shutdown_requested()) {
    amqp_maybe_release_buffers(consumer->connection);
    amqp_envelope_t envelope = {0};
    amqp_rpc_reply_t reply =
        amqp_consume_message(consumer->connection, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      http_delivery_result_t delivery = http_webhook_deliver(
          webhook, envelope.message.body.bytes, envelope.message.body.len);
      http_transmitter_status_t settlement =
          http_settle_delivery(consumer, envelope.delivery_tag, delivery);
      amqp_destroy_envelope(&envelope);
      if (settlement != HTTP_TRANSMITTER_STATUS_OK) {
        return settlement;
      }
      if (delivery == HTTP_DELIVERY_SHUTDOWN) {
        break;
      }
      continue;
    }
    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      continue;
    }
    http_log_rpc_failure("consume message", reply);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}
