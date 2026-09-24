#pragma once

#include "websocket-client/monitor_config.h"

#include <stdint.h>

/**
 * Runtime trust boundary:
 * Remote WebSocket, Redis, and RabbitMQ peers are untrusted. Protocol parsing
 * remains in the integration modules; this layer owns shutdown and bounded
 * reconnect orchestration and never logs credentials.
 */
typedef enum monitor_runtime_status : uint8_t {
  MONITOR_RUNTIME_STATUS_OK = 0,
  MONITOR_RUNTIME_STATUS_INVALID_CONFIG,
  MONITOR_RUNTIME_STATUS_SIGNAL_ERROR,
  MONITOR_RUNTIME_STATUS_SUBSCRIBER_ERROR,
} monitor_runtime_status_t;

[[nodiscard]] monitor_runtime_status_t
app_runtime_run(const monitor_config_t *config);
