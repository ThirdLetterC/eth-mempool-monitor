#include "websocket-client/monitor_runtime.h"

#include "websocket-client/subscriber.h"

#include "ulog/ulog.h"

#include <errno.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/*
 * Runtime orchestration for the monitor process.
 *
 * Signal handlers only set a sig_atomic_t flag. Normal control flow observes
 * that flag, closes network resources, and performs logging outside signal
 * context.
 */
static volatile sig_atomic_t app_shutdown_signal = 0;

static void app_handle_shutdown_signal(int signal_number) {
  app_shutdown_signal = signal_number;
}

[[nodiscard]] static bool app_is_shutdown_requested() {
  return app_shutdown_signal != 0;
}

[[nodiscard]] static const char *app_signal_name(int signal_number) {
  switch (signal_number) {
  case SIGINT:
    return "SIGINT";
  case SIGTERM:
    return "SIGTERM";
  default:
    return "signal";
  }
}

[[nodiscard]] static bool app_install_signal_handlers() {
  struct sigaction action = {0};
  action.sa_handler = app_handle_shutdown_signal;
  if (sigemptyset(&action.sa_mask) != 0) {
    ulog_error("Failed to initialize signal handler mask");
    return false;
  }

  if (sigaction(SIGINT, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGINT handler: %s", strerror(errno));
    return false;
  }
  if (sigaction(SIGTERM, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGTERM handler: %s", strerror(errno));
    return false;
  }
  return true;
}

[[nodiscard]] static bool app_sleep_milliseconds(uint32_t duration_ms) {
  struct timespec request = {
      .tv_sec = (time_t)(duration_ms / 1'000U),
      .tv_nsec = (long)((duration_ms % 1'000U) * 1'000'000U),
  };
  struct timespec remaining = {0};

  while (nanosleep(&request, &remaining) != 0) {
    if (errno != EINTR) {
      ulog_warn("Backoff sleep failed: %s", strerror(errno));
      return false;
    }
    if (app_is_shutdown_requested()) {
      return false;
    }
    request = remaining;
  }

  return !app_is_shutdown_requested();
}

static void app_log_startup_config(const monitor_config_t *config) {
  ulog_info("Starting websocket subscriber (log_level=%s color=%s)",
            ulog_level_to_string(config->log_level),
            config->log_color ? "true" : "false");
  ulog_info("WebSocket endpoint: %s://%s:%u%s", config->secure ? "wss" : "ws",
            config->host, (unsigned)config->port.value, config->path);
  ulog_info("WebSocket timeouts: read=%u sec write=%u sec",
            (unsigned)config->read_timeout.value,
            (unsigned)config->write_timeout.value);
  if (config->reconnect_enabled) {
    ulog_info("Reconnect policy: enabled (initial=%u ms, max=%u ms)",
              (unsigned)config->reconnect_initial_backoff.value,
              (unsigned)config->reconnect_max_backoff.value);
  } else {
    ulog_info("Reconnect policy: disabled");
  }
  ulog_info("Redis monitor: %s:%u key=%s", config->redis_host,
            (unsigned)config->redis_port.value,
            config->redis_monitored_set_key);
  if (config->rabbitmq_enabled) {
    ulog_info("RabbitMQ target: %s:%u vhost=%s queue=%s durable=%s channel=%u "
              "heartbeat=%u sec",
              config->rabbitmq_host, (unsigned)config->rabbitmq_port.value,
              config->rabbitmq_vhost, config->rabbitmq_queue,
              config->rabbitmq_queue_durable ? "true" : "false",
              (unsigned)config->rabbitmq_channel.value,
              (unsigned)config->rabbitmq_heartbeat.value);
  } else {
    ulog_info(
        "RabbitMQ publishing disabled by config (rabbitmq.enabled=false)");
  }
}

[[nodiscard]] monitor_runtime_status_t
app_runtime_run(const monitor_config_t *config) {
  if (config == nullptr) {
    return MONITOR_RUNTIME_STATUS_INVALID_CONFIG;
  }
  if (!app_install_signal_handlers()) {
    return MONITOR_RUNTIME_STATUS_SIGNAL_ERROR;
  }

  app_log_startup_config(config);

  ws_subscriber_redis_config_t redis_config = {
      .server =
          {
              .host = config->redis_host,
              .port = config->redis_port,
          },
      .monitored_set_key = config->redis_monitored_set_key,
  };
  ws_rabbitmq_config_t rabbitmq_config = {
      .server =
          {
              .host = config->rabbitmq_host,
              .port = config->rabbitmq_port,
          },
      .username = config->rabbitmq_username,
      .password = config->rabbitmq_password,
      .vhost = config->rabbitmq_vhost,
      .queue = config->rabbitmq_queue,
      .queue_durable = config->rabbitmq_queue_durable,
      .channel = config->rabbitmq_channel,
      .heartbeat = config->rabbitmq_heartbeat,
  };

  app_milliseconds_t reconnect_backoff = config->reconnect_initial_backoff;
  size_t reconnect_attempt = 0;
  ws_subscriber_status_t status = WS_SUBSCRIBER_STATUS_WEBSOCKET_ERROR;

  ws_subscriber_options_t subscriber_options = {
      .websocket =
          {
              .server =
                  {
                      .host = config->host,
                      .port = config->port,
                  },
              .path = config->path,
              .transport =
                  config->secure ? WS_TRANSPORT_TLS : WS_TRANSPORT_PLAIN,
          },
      .timeouts =
          {
              .read = config->read_timeout,
              .write = config->write_timeout,
          },
      .subscribe_request = config->request,
      .redis = &redis_config,
      .rabbitmq = config->rabbitmq_enabled ? &rabbitmq_config : nullptr,
      .should_stop = app_is_shutdown_requested,
  };

  /* A successful run or an explicit shutdown terminates the reconnect loop. */
  while (true) {
    status = ws_subscriber_run(&subscriber_options);
    if (!ws_subscriber_status_is_retryable(status) ||
        app_is_shutdown_requested() || !config->reconnect_enabled) {
      break;
    }

    ++reconnect_attempt;
    ulog_warn(
        "Subscriber exited unexpectedly, reconnecting (attempt=%zu) in %u ms",
        reconnect_attempt, (unsigned)reconnect_backoff.value);
    if (!app_sleep_milliseconds(reconnect_backoff.value)) {
      break;
    }

    /* Saturating exponential backoff avoids overflow and reconnect storms. */
    if (reconnect_backoff.value < config->reconnect_max_backoff.value) {
      uint64_t next_backoff = (uint64_t)reconnect_backoff.value * 2U;
      reconnect_backoff.value =
          (next_backoff > config->reconnect_max_backoff.value)
              ? config->reconnect_max_backoff.value
              : (uint32_t)next_backoff;
    }
  }
  if (app_is_shutdown_requested()) {
    ulog_info("Received %s, shutting down gracefully",
              app_signal_name((int)app_shutdown_signal));
    return MONITOR_RUNTIME_STATUS_OK;
  }

  return status == WS_SUBSCRIBER_STATUS_OK ||
                 status == WS_SUBSCRIBER_STATUS_STOPPED
             ? MONITOR_RUNTIME_STATUS_OK
             : MONITOR_RUNTIME_STATUS_SUBSCRIBER_ERROR;
}
