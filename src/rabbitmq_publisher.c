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

  if (!ws_rabbitmq_worker_start(publisher)) {
    ws_rabbitmq_cleanup_worker_resources(publisher);
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
  if (!ws_rabbitmq_replay_enqueue(publisher, payload, payload_length)) {
    return false;
  }
  int status = uv_async_send(&publisher->work_async);
  if (status != 0) {
    ulog_error("Failed to signal RabbitMQ publisher worker: %s",
               uv_strerror(status));
    return false;
  }
  return true;
}

void ws_rabbitmq_publisher_destroy(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  ws_rabbitmq_cleanup_worker_resources(publisher);
  ws_rabbitmq_cleanup_owned_fields(publisher);
  free(publisher);
}
