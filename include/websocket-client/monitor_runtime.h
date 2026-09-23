#pragma once

#include "websocket-client/monitor_config.h"

/**
 * Runtime trust boundary:
 * Remote WebSocket, Redis, and RabbitMQ peers are untrusted. Protocol parsing
 * remains in the integration modules; this layer owns shutdown and bounded
 * reconnect orchestration and never logs credentials.
 */
[[nodiscard]] bool app_runtime_run(const app_config *config);
