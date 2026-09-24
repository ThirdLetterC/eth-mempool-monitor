#pragma once

#include "app/domain_types.h"

#include <stddef.h>
#include <stdint.h>

typedef struct ws_rabbitmq_config ws_rabbitmq_config_t;
struct ws_rabbitmq_config {
  app_tcp_endpoint_t server;
  const char *username;
  const char *password;
  const char *vhost;
  const char *queue;
  bool queue_durable;
  app_rabbitmq_channel_t channel;
  app_seconds_t heartbeat;
};

typedef struct ws_rabbitmq_publisher ws_rabbitmq_publisher_t;

typedef enum ws_rabbitmq_status : uint8_t {
  WS_RABBITMQ_STATUS_OK = 0,
  WS_RABBITMQ_STATUS_INVALID_ARGUMENT,
  WS_RABBITMQ_STATUS_ALLOCATION_FAILED,
  WS_RABBITMQ_STATUS_QUEUE_FULL,
  WS_RABBITMQ_STATUS_WORKER_ERROR,
  WS_RABBITMQ_STATUS_CONNECTION_ERROR,
  WS_RABBITMQ_STATUS_PUBLISH_ERROR,
  WS_RABBITMQ_STATUS_CONFIRM_ERROR,
  WS_RABBITMQ_STATUS_SIGNAL_ERROR,
} ws_rabbitmq_status_t;

static_assert(sizeof(ws_rabbitmq_status_t) == sizeof(uint8_t));

/**
 * @brief Connects to RabbitMQ and declares the configured queue.
 * @param out_publisher receives the owning pointer on success.
 * @return typed validation, allocation, connection, or worker status.
 */
[[nodiscard]] ws_rabbitmq_status_t
ws_rabbitmq_publisher_create(const ws_rabbitmq_config_t *config,
                             ws_rabbitmq_publisher_t **out_publisher);

/**
 * @brief Publishes one message payload to the configured queue.
 */
[[nodiscard]] ws_rabbitmq_status_t
ws_rabbitmq_publisher_publish(ws_rabbitmq_publisher_t *publisher,
                              const char *payload, size_t payload_length);

/**
 * @brief Closes RabbitMQ channel and connection.
 */
void ws_rabbitmq_publisher_destroy(ws_rabbitmq_publisher_t *publisher);

[[nodiscard]] const char *
ws_rabbitmq_status_string(ws_rabbitmq_status_t status);
