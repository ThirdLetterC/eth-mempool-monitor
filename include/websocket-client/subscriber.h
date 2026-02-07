#ifndef WEBSOCKET_CLIENT_SUBSCRIBER_H
#define WEBSOCKET_CLIENT_SUBSCRIBER_H

#include "websocket-client/rabbitmq_publisher.h"
#include "websocket-client/ws_client.h"

#include <stdint.h>

/**
 * @brief Continuously listens for text frames and processes subscription
 * messages.
 * @return false when receive fails or the connection is closed.
 */
[[nodiscard]] bool ws_subscriber_listen(ws_client_t *client);

typedef bool (*ws_subscriber_stop_check_fn)(void);

typedef struct ws_subscriber_redis_config ws_subscriber_redis_config_t;
struct ws_subscriber_redis_config {
  const char *host;
  uint16_t port;
  const char *monitored_set_key;
};

/**
 * @brief Sets a callback used to check whether subscriber loops should stop.
 */
void ws_subscriber_set_stop_check(ws_subscriber_stop_check_fn stop_check);

/**
 * @brief Connects over wss://, sends a subscription request, then starts
 * listening.
 * @return false on setup or receive failure.
 */
[[nodiscard]] bool ws_subscriber_run(const char *host, uint16_t port,
                                     const char *path,
                                     const char *subscribe_request);

/**
 * @brief Connects over ws:// or wss://, sends a subscription request, then
 * starts listening.
 * @return false on setup or receive failure.
 */
[[nodiscard]] bool ws_subscriber_run_ex(const char *host, uint16_t port,
                                        const char *path,
                                        const char *subscribe_request,
                                        bool secure);

/**
 * @brief Connects to websocket endpoint, checks tx addresses against Redis,
 * then listens.
 * @return false on setup or receive failure.
 */
[[nodiscard]] bool ws_subscriber_run_ex_with_redis(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config);

/**
 * @brief Connects to websocket endpoint, checks tx addresses against Redis,
 * publishes matches to RabbitMQ.
 * @return false on setup or receive failure.
 */
[[nodiscard]] bool ws_subscriber_run_ex_with_integrations(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config,
    const ws_rabbitmq_config_t *rabbitmq_config);

/**
 * @brief Same as ws_subscriber_run_ex_with_integrations but overrides websocket
 * socket timeouts.
 * @param read_timeout_seconds socket receive timeout in seconds (must be >0).
 * @param write_timeout_seconds socket send timeout in seconds (must be >0).
 */
[[nodiscard]] bool ws_subscriber_run_ex_with_integrations_and_timeouts(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config,
    const ws_rabbitmq_config_t *rabbitmq_config, uint32_t read_timeout_seconds,
    uint32_t write_timeout_seconds);

#endif
