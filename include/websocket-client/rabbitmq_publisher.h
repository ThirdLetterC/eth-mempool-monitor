#pragma once

#include <stddef.h>
#include <stdint.h>

typedef struct ws_rabbitmq_config ws_rabbitmq_config_t;
struct ws_rabbitmq_config {
  const char *host;
  uint16_t port;
  const char *username;
  const char *password;
  const char *vhost;
  const char *queue;
  bool queue_durable;
  uint16_t channel;
  uint16_t heartbeat_seconds;
};

typedef struct ws_rabbitmq_publisher ws_rabbitmq_publisher_t;

/**
 * @brief Connects to RabbitMQ and declares the configured queue.
 * @return owning pointer or nullptr on failure.
 */
[[nodiscard]] ws_rabbitmq_publisher_t *
ws_rabbitmq_publisher_create(const ws_rabbitmq_config_t *config);

/**
 * @brief Publishes one message payload to the configured queue.
 */
[[nodiscard]] bool
ws_rabbitmq_publisher_publish(ws_rabbitmq_publisher_t *publisher,
                              const char *payload, size_t payload_length);

/**
 * @brief Closes RabbitMQ channel and connection.
 */
void ws_rabbitmq_publisher_destroy(ws_rabbitmq_publisher_t *publisher);
