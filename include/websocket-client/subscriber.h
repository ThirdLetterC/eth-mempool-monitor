#pragma once

#include "websocket-client/rabbitmq_publisher.h"
#include "websocket-client/ws_client.h"

#include <stdint.h>

typedef bool (*ws_subscriber_stop_check_fn)();

typedef struct ws_subscriber_redis_config ws_subscriber_redis_config_t;
struct ws_subscriber_redis_config {
  app_tcp_endpoint_t server;
  const char *monitored_set_key;
};

typedef enum ws_subscriber_status : uint8_t {
  WS_SUBSCRIBER_STATUS_OK = 0,
  WS_SUBSCRIBER_STATUS_STOPPED,
  WS_SUBSCRIBER_STATUS_INVALID_CONFIG,
  WS_SUBSCRIBER_STATUS_REDIS_ERROR,
  WS_SUBSCRIBER_STATUS_RABBITMQ_ERROR,
  WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR,
  WS_SUBSCRIBER_STATUS_RECONNECT_REQUIRED,
} ws_subscriber_status_t;

static_assert(sizeof(ws_subscriber_status_t) == sizeof(uint8_t));

typedef struct {
  ws_endpoint_t websocket;
  ws_timeouts_t timeouts;
  const char *subscribe_request;
  const ws_subscriber_redis_config_t *redis;
  const ws_rabbitmq_config_t *rabbitmq;
  ws_subscriber_stop_check_fn should_stop;
} ws_subscriber_options_t;

/**
 * @brief Runs one fully configured subscription attempt.
 * Optional integration pointers and the stop callback are borrowed for the
 * duration of the call.
 */
[[nodiscard]] ws_subscriber_status_t
ws_subscriber_run(const ws_subscriber_options_t *options);

[[nodiscard]] bool
ws_subscriber_status_is_retryable(ws_subscriber_status_t status);

[[nodiscard]] const char *
ws_subscriber_status_string(ws_subscriber_status_t status);
