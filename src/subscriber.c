#include "websocket-client/subscriber.h"
#include "websocket-client/subscriber_internal.h"

#include "ulog/ulog.h"

#include <string.h>

/*
 * Subscriber lifecycle and resource-ownership layer.
 *
 * This module acquires Redis, RabbitMQ, and WebSocket resources, drives the
 * receive loop, and releases every successfully acquired resource on exit.
 * Message interpretation is isolated in subscriber_message.c.
 */
constexpr size_t WS_SUBSCRIBER_MESSAGE_CAPACITY = 256 * 1'024;
[[nodiscard]] static bool
ws_subscriber_should_stop(const ws_subscriber_options_t *options) {
  return options->should_stop != nullptr && options->should_stop();
}

[[nodiscard]] static ws_subscriber_status_t
ws_subscriber_listen_ex(ws_client_t *client,
                        ws_subscriber_runtime_config_t *runtime_config,
                        const ws_subscriber_options_t *options) {
  if (client == nullptr) {
    return WS_SUBSCRIBER_STATUS_INVALID_CONFIG;
  }

  char message[WS_SUBSCRIBER_MESSAGE_CAPACITY] = {0};
  while (true) {
    if (ws_subscriber_should_stop(options)) {
      return WS_SUBSCRIBER_STATUS_STOPPED;
    }

    size_t message_length = 0;
    auto receive_status = ws_client_receive_text(
        client, message, sizeof(message), &message_length);
    if (receive_status != WS_STATUS_OK) {
      if (ws_subscriber_should_stop(options)) {
        return WS_SUBSCRIBER_STATUS_STOPPED;
      }
      if (receive_status == WS_STATUS_BUFFER_TOO_SMALL) {
        ulog_warn("Skipping oversized websocket message: %s",
                  ws_client_last_error(client));
        continue;
      }
      return WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR;
    }

    auto action =
        ws_subscriber_handle_message(message, message_length, runtime_config);
    if (action == WS_SUBSCRIBER_MESSAGE_ACTION_RECONNECT) {
      /* monitor_runtime owns retry policy after this attempt unwinds. */
      ws_client_close(client);
      return WS_SUBSCRIBER_STATUS_RECONNECT_REQUIRED;
    }
  }
}

[[nodiscard]] ws_subscriber_status_t
ws_subscriber_run(const ws_subscriber_options_t *options) {
  if (options == nullptr || options->websocket.server.host == nullptr ||
      options->websocket.server.port.value == 0 ||
      options->websocket.path == nullptr ||
      options->subscribe_request == nullptr ||
      options->timeouts.read.value == 0 || options->timeouts.write.value == 0) {
    ulog_error("Invalid subscriber input");
    return WS_SUBSCRIBER_STATUS_INVALID_CONFIG;
  }

  /* Resources are acquired in dependency order and remain owned locally. */
  ws_subscriber_runtime_config_t runtime_config = {0};
  if (options->redis != nullptr) {
    if (options->redis->server.host == nullptr ||
        options->redis->monitored_set_key == nullptr ||
        options->redis->server.port.value == 0) {
      ulog_error("Invalid Redis configuration");
      return WS_SUBSCRIBER_STATUS_INVALID_CONFIG;
    }

    runtime_config.redis = redisConnect(options->redis->server.host,
                                        (int)options->redis->server.port.value);
    if (runtime_config.redis == nullptr) {
      ulog_error("Failed to allocate Redis context");
      return WS_SUBSCRIBER_STATUS_REDIS_ERROR;
    }

    if (runtime_config.redis->err != 0) {
      ulog_error("Failed to connect to Redis at %s:%u: %s",
                 options->redis->server.host,
                 (unsigned)options->redis->server.port.value,
                 runtime_config.redis->errstr);
      redisFree(runtime_config.redis);
      return WS_SUBSCRIBER_STATUS_REDIS_ERROR;
    }

    runtime_config.monitored_set_key = options->redis->monitored_set_key;
    ulog_info("Connected to Redis at %s:%u (set=%s)",
              options->redis->server.host,
              (unsigned)options->redis->server.port.value,
              runtime_config.monitored_set_key);
  }

  if (options->rabbitmq != nullptr) {
    auto rabbitmq_status = ws_rabbitmq_publisher_create(
        options->rabbitmq, &runtime_config.rabbitmq_publisher);
    if (rabbitmq_status != WS_RABBITMQ_STATUS_OK) {
      if (runtime_config.redis != nullptr) {
        redisFree(runtime_config.redis);
      }
      return WS_SUBSCRIBER_STATUS_RABBITMQ_ERROR;
    }
  }

  ws_client_t *client = nullptr;
  if (ws_client_create(&client) != WS_STATUS_OK) {
    ulog_error("Failed to allocate websocket client");
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR;
  }
  runtime_config.client = client;
  runtime_config.next_tx_lookup_request_id =
      WS_SUBSCRIBER_INITIAL_TX_LOOKUP_REQUEST_ID;

  if (ws_client_set_timeouts(client, options->timeouts) != WS_STATUS_OK) {
    ulog_error("Invalid websocket timeout configuration (read=%u write=%u)",
               (unsigned)options->timeouts.read.value,
               (unsigned)options->timeouts.write.value);
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return WS_SUBSCRIBER_STATUS_INVALID_CONFIG;
  }

  const char *scheme =
      options->websocket.transport == WS_TRANSPORT_TLS ? "wss" : "ws";
  ulog_info(
      "Connecting to %s://%s:%u%s", scheme, options->websocket.server.host,
      (unsigned)options->websocket.server.port.value, options->websocket.path);

  auto connect_status = ws_client_connect(client, &options->websocket);
  if (connect_status != WS_STATUS_OK) {
    ulog_error("WebSocket connect failed (%s://%s:%u%s): %s", scheme,
               options->websocket.server.host,
               (unsigned)options->websocket.server.port.value,
               options->websocket.path, ws_client_last_error(client));
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR;
  }

  if (ws_client_send_text(client, options->subscribe_request,
                          strlen(options->subscribe_request)) != WS_STATUS_OK) {
    ulog_error("Subscription send failed: %s", ws_client_last_error(client));
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR;
  }

  ulog_info("Connected to %s://%s:%u%s", scheme, options->websocket.server.host,
            (unsigned)options->websocket.server.port.value,
            options->websocket.path);
  ulog_info("Subscription request sent (%zu bytes)",
            strlen(options->subscribe_request));

  auto listen_status =
      ws_subscriber_listen_ex(client, &runtime_config, options);
  if (listen_status != WS_SUBSCRIBER_STATUS_STOPPED) {
    ulog_error("Subscriber stopped: %s", ws_client_last_error(client));
  } else {
    ulog_info("Shutdown requested, closing subscriber");
  }

  /* Reverse-order teardown also handles a normal stop request. */
  ws_client_destroy(client);
  if (runtime_config.redis != nullptr) {
    redisFree(runtime_config.redis);
  }
  if (runtime_config.rabbitmq_publisher != nullptr) {
    ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
  }
  return listen_status;
}

[[nodiscard]] bool
ws_subscriber_status_is_retryable(ws_subscriber_status_t status) {
  return status == WS_SUBSCRIBER_STATUS_REDIS_ERROR ||
         status == WS_SUBSCRIBER_STATUS_RABBITMQ_ERROR ||
         status == WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR ||
         status == WS_SUBSCRIBER_STATUS_RECONNECT_REQUIRED;
}

[[nodiscard]] const char *
ws_subscriber_status_string(ws_subscriber_status_t status) {
  switch (status) {
  case WS_SUBSCRIBER_STATUS_OK:
    return "ok";
  case WS_SUBSCRIBER_STATUS_STOPPED:
    return "stopped";
  case WS_SUBSCRIBER_STATUS_INVALID_CONFIG:
    return "invalid configuration";
  case WS_SUBSCRIBER_STATUS_REDIS_ERROR:
    return "Redis error";
  case WS_SUBSCRIBER_STATUS_RABBITMQ_ERROR:
    return "RabbitMQ error";
  case WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR:
    return "WebSocket error";
  case WS_SUBSCRIBER_STATUS_RECONNECT_REQUIRED:
    return "reconnect required";
  }
  return "unknown subscriber status";
}
