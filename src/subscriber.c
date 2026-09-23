#include "websocket-client/subscriber.h"
#include "websocket-client/subscriber_internal.h"

#include "hiredis/hiredis.h"
#include "ulog/ulog.h"

#include <string.h>

/*
 * Subscriber lifecycle and resource-ownership layer.
 *
 * This module acquires Redis, RabbitMQ, and WebSocket resources, drives the
 * receive loop, and releases every successfully acquired resource on exit.
 * Message interpretation is isolated in subscriber_message.c.
 */
constexpr size_t WS_SUBSCRIBER_MESSAGE_CAPACITY = 64 * 1024;
constexpr char WS_SUBSCRIBER_ERR_BUFFER_TOO_SMALL_PREFIX[] =
    "Receive buffer is too small";

static ws_subscriber_stop_check_fn ws_subscriber_stop_check = nullptr;

[[nodiscard]] static bool ws_subscriber_should_stop() {
  return ws_subscriber_stop_check != nullptr && ws_subscriber_stop_check();
}

[[nodiscard]] static bool
ws_subscriber_is_oversized_message_error(const char *error_message) {
  if (error_message == nullptr) {
    return false;
  }
  return strncmp(error_message, WS_SUBSCRIBER_ERR_BUFFER_TOO_SMALL_PREFIX,
                 sizeof(WS_SUBSCRIBER_ERR_BUFFER_TOO_SMALL_PREFIX) - 1) == 0;
}

[[nodiscard]] static bool
ws_subscriber_listen_ex(ws_client_t *client,
                        ws_subscriber_runtime_config_t *runtime_config) {
  if (client == nullptr) {
    return false;
  }

  char message[WS_SUBSCRIBER_MESSAGE_CAPACITY] = {0};
  while (true) {
    if (ws_subscriber_should_stop()) {
      return true;
    }

    size_t message_length = 0;
    if (!ws_client_receive_text(client, message, sizeof(message),
                                &message_length)) {
      if (ws_subscriber_should_stop()) {
        return true;
      }
      auto error_message = ws_client_last_error(client);
      if (ws_subscriber_is_oversized_message_error(error_message)) {
        ulog_warn("Skipping oversized websocket message: %s", error_message);
        continue;
      }
      return false;
    }

    auto action =
        ws_subscriber_handle_message(message, message_length, runtime_config);
    if (action == WS_SUBSCRIBER_MESSAGE_ACTION_RECONNECT) {
      /* monitor_runtime owns retry policy after this attempt unwinds. */
      ws_client_close(client);
      return false;
    }
  }
}

[[nodiscard]] bool ws_subscriber_listen(ws_client_t *client) {
  return ws_subscriber_listen_ex(client, nullptr);
}

void ws_subscriber_set_stop_check(ws_subscriber_stop_check_fn stop_check) {
  ws_subscriber_stop_check = stop_check;
}

[[nodiscard]] bool ws_subscriber_run(const char *host, uint16_t port,
                                     const char *path,
                                     const char *subscribe_request) {
  return ws_subscriber_run_ex(host, port, path, subscribe_request, true);
}

[[nodiscard]] bool ws_subscriber_run_ex(const char *host, uint16_t port,
                                        const char *path,
                                        const char *subscribe_request,
                                        bool secure) {
  return ws_subscriber_run_ex_with_integrations(
      host, port, path, subscribe_request, secure, nullptr, nullptr);
}

[[nodiscard]] bool ws_subscriber_run_ex_with_redis(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config) {
  return ws_subscriber_run_ex_with_integrations(
      host, port, path, subscribe_request, secure, redis_config, nullptr);
}

[[nodiscard]] bool ws_subscriber_run_ex_with_integrations(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config,
    const ws_rabbitmq_config_t *rabbitmq_config) {
  return ws_subscriber_run_ex_with_integrations_and_timeouts(
      host, port, path, subscribe_request, secure, redis_config,
      rabbitmq_config, 0, 0);
}

[[nodiscard]] bool ws_subscriber_run_ex_with_integrations_and_timeouts(
    const char *host, uint16_t port, const char *path,
    const char *subscribe_request, bool secure,
    const ws_subscriber_redis_config_t *redis_config,
    const ws_rabbitmq_config_t *rabbitmq_config, uint32_t read_timeout_seconds,
    uint32_t write_timeout_seconds) {
  if (host == nullptr || path == nullptr || subscribe_request == nullptr) {
    ulog_error("Invalid subscriber input");
    return false;
  }

  /* Resources are acquired in dependency order and remain owned locally. */
  ws_subscriber_runtime_config_t runtime_config = {0};
  if (redis_config != nullptr) {
    if (redis_config->host == nullptr ||
        redis_config->monitored_set_key == nullptr || redis_config->port == 0) {
      ulog_error("Invalid Redis configuration");
      return false;
    }

    runtime_config.redis =
        redisConnect(redis_config->host, (int)redis_config->port);
    if (runtime_config.redis == nullptr) {
      ulog_error("Failed to allocate Redis context");
      return false;
    }

    if (runtime_config.redis->err != 0) {
      ulog_error("Failed to connect to Redis at %s:%u: %s", redis_config->host,
                 redis_config->port, runtime_config.redis->errstr);
      redisFree(runtime_config.redis);
      return false;
    }

    runtime_config.monitored_set_key = redis_config->monitored_set_key;
    ulog_info("Connected to Redis at %s:%u (set=%s)", redis_config->host,
              redis_config->port, runtime_config.monitored_set_key);
  }

  if (rabbitmq_config != nullptr) {
    runtime_config.rabbitmq_publisher =
        ws_rabbitmq_publisher_create(rabbitmq_config);
    if (runtime_config.rabbitmq_publisher == nullptr) {
      if (runtime_config.redis != nullptr) {
        redisFree(runtime_config.redis);
      }
      return false;
    }
  }

  ws_client_t *client = ws_client_create();
  if (client == nullptr) {
    ulog_error("Failed to allocate websocket client");
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return false;
  }
  runtime_config.client = client;
  runtime_config.next_tx_lookup_request_id =
      WS_SUBSCRIBER_INITIAL_TX_LOOKUP_REQUEST_ID;

  if ((read_timeout_seconds > 0 || write_timeout_seconds > 0) &&
      !ws_client_set_timeouts(client, read_timeout_seconds,
                              write_timeout_seconds)) {
    ulog_error("Invalid websocket timeout configuration (read=%u write=%u)",
               (unsigned)read_timeout_seconds, (unsigned)write_timeout_seconds);
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return false;
  }

  ulog_info("Connecting to %s://%s:%u%s", secure ? "wss" : "ws", host, port,
            path);

  auto connected = secure ? ws_client_connect_secure(client, host, port, path)
                          : ws_client_connect(client, host, port, path);
  if (!connected) {
    ulog_error("%s connect failed (%s://%s:%u%s): %s", secure ? "WSS" : "WS",
               secure ? "wss" : "ws", host, port, path,
               ws_client_last_error(client));
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return false;
  }

  if (!ws_client_send_text(client, subscribe_request,
                           strlen(subscribe_request))) {
    ulog_error("Subscription send failed: %s", ws_client_last_error(client));
    ws_client_destroy(client);
    if (runtime_config.redis != nullptr) {
      redisFree(runtime_config.redis);
    }
    if (runtime_config.rabbitmq_publisher != nullptr) {
      ws_rabbitmq_publisher_destroy(runtime_config.rabbitmq_publisher);
    }
    return false;
  }

  ulog_info("Connected to %s://%s:%u%s", secure ? "wss" : "ws", host, port,
            path);
  ulog_info("Subscription request sent (%zu bytes)", strlen(subscribe_request));

  auto listen_ok = ws_subscriber_listen_ex(client, &runtime_config);
  if (!listen_ok) {
    ulog_error("Subscriber stopped: %s", ws_client_last_error(client));
  } else if (ws_subscriber_should_stop()) {
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
  return listen_ok;
}
