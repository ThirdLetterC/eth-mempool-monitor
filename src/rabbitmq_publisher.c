#include "websocket-client/rabbitmq_publisher_internal.h"

#include "ulog/ulog.h"

#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

constexpr amqp_channel_t WS_RABBITMQ_DEFAULT_CHANNEL = 1;
constexpr uint16_t WS_RABBITMQ_DEFAULT_HEARTBEAT_SECONDS = 30;
constexpr char WS_RABBITMQ_CONTENT_TYPE[] = "application/json";

/*
 * Publisher lifecycle trust boundary:
 * Configuration and payloads are caller-controlled. Configuration is copied
 * into owned storage before broker operations or replay queuing begins.
 */

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

static void
ws_rabbitmq_cleanup_worker_resources(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  ws_rabbitmq_worker_stop(publisher);
  ws_rabbitmq_replay_clear(publisher);
  if (publisher->condition_initialized) {
    uv_cond_destroy(&publisher->startup_condition);
    publisher->condition_initialized = false;
  }
  if (publisher->mutex_initialized) {
    uv_mutex_destroy(&publisher->queue_mutex);
    publisher->mutex_initialized = false;
  }
}

[[nodiscard]] ws_rabbitmq_status_t
ws_rabbitmq_publisher_create(const ws_rabbitmq_config_t *config,
                             ws_rabbitmq_publisher_t **out_publisher) {
  if (out_publisher == nullptr) {
    return WS_RABBITMQ_STATUS_INVALID_ARGUMENT;
  }
  *out_publisher = nullptr;
  if (config == nullptr || config->server.host == nullptr ||
      config->server.port.value == 0 || config->username == nullptr ||
      config->password == nullptr || config->vhost == nullptr ||
      config->queue == nullptr || config->heartbeat.value > UINT16_MAX) {
    ulog_error("Invalid RabbitMQ configuration");
    return WS_RABBITMQ_STATUS_INVALID_ARGUMENT;
  }

  ws_rabbitmq_publisher_t *publisher =
      calloc(1, sizeof(ws_rabbitmq_publisher_t));
  if (publisher == nullptr) {
    ulog_error("Failed to allocate RabbitMQ publisher");
    return WS_RABBITMQ_STATUS_ALLOCATION_FAILED;
  }

  publisher->host_owned = ws_rabbitmq_string_duplicate(config->server.host);
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
    return WS_RABBITMQ_STATUS_ALLOCATION_FAILED;
  }

  publisher->host = publisher->host_owned;
  publisher->username = publisher->username_owned;
  publisher->password = publisher->password_owned;
  publisher->vhost = publisher->vhost_owned;
  publisher->queue = publisher->queue_owned;
  publisher->port = config->server.port.value;
  publisher->queue_durable = config->queue_durable;
  publisher->channel = config->channel.value != 0
                           ? config->channel.value
                           : (uint16_t)WS_RABBITMQ_DEFAULT_CHANNEL;
  publisher->heartbeat_seconds = config->heartbeat.value != 0
                                     ? (uint16_t)config->heartbeat.value
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

  if (!ws_rabbitmq_worker_start(publisher)) {
    ws_rabbitmq_cleanup_worker_resources(publisher);
    ws_rabbitmq_cleanup_owned_fields(publisher);
    free(publisher);
    return WS_RABBITMQ_STATUS_WORKER_ERROR;
  }

  *out_publisher = publisher;
  return WS_RABBITMQ_STATUS_OK;
}

[[nodiscard]] ws_rabbitmq_status_t
ws_rabbitmq_publisher_publish(ws_rabbitmq_publisher_t *publisher,
                              const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return WS_RABBITMQ_STATUS_INVALID_ARGUMENT;
  }
  if (!ws_rabbitmq_replay_enqueue(publisher, payload, payload_length)) {
    return WS_RABBITMQ_STATUS_QUEUE_FULL;
  }
  int status = uv_async_send(&publisher->work_async);
  if (status != 0) {
    ulog_error("Failed to signal RabbitMQ publisher worker: %s",
               uv_strerror(status));
    return WS_RABBITMQ_STATUS_SIGNAL_ERROR;
  }
  return WS_RABBITMQ_STATUS_OK;
}

void ws_rabbitmq_publisher_destroy(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  ws_rabbitmq_cleanup_worker_resources(publisher);
  ws_rabbitmq_cleanup_owned_fields(publisher);
  free(publisher);
}

[[nodiscard]] const char *
ws_rabbitmq_status_string(ws_rabbitmq_status_t status) {
  switch (status) {
  case WS_RABBITMQ_STATUS_OK:
    return "ok";
  case WS_RABBITMQ_STATUS_INVALID_ARGUMENT:
    return "invalid argument";
  case WS_RABBITMQ_STATUS_ALLOCATION_FAILED:
    return "allocation failed";
  case WS_RABBITMQ_STATUS_QUEUE_FULL:
    return "queue full";
  case WS_RABBITMQ_STATUS_WORKER_ERROR:
    return "worker error";
  case WS_RABBITMQ_STATUS_CONNECTION_ERROR:
    return "connection error";
  case WS_RABBITMQ_STATUS_PUBLISH_ERROR:
    return "publish error";
  case WS_RABBITMQ_STATUS_CONFIRM_ERROR:
    return "confirmation error";
  case WS_RABBITMQ_STATUS_SIGNAL_ERROR:
    return "signal error";
  }
  return "unknown RabbitMQ status";
}
