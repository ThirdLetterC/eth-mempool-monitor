#define _POSIX_C_SOURCE 200809L

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

static void app_log_startup_config(const app_config *config) {
  ulog_info("Starting websocket subscriber (log_level=%s color=%s)",
            ulog_level_to_string(config->log_level),
            config->log_color ? "true" : "false");
  ulog_info("WebSocket endpoint: %s://%s:%u%s", config->secure ? "wss" : "ws",
            config->host, config->port, config->path);
  ulog_info("WebSocket timeouts: read=%u sec write=%u sec",
            (unsigned)config->read_timeout_seconds,
            (unsigned)config->write_timeout_seconds);
  if (config->reconnect_enabled) {
    ulog_info("Reconnect policy: enabled (initial=%u ms, max=%u ms)",
              (unsigned)config->reconnect_initial_backoff_ms,
              (unsigned)config->reconnect_max_backoff_ms);
  } else {
    ulog_info("Reconnect policy: disabled");
  }
  ulog_info("Redis monitor: %s:%u key=%s", config->redis_host,
            config->redis_port, config->redis_monitored_set_key);
  if (config->rabbitmq_enabled) {
    ulog_info("RabbitMQ target: %s:%u vhost=%s queue=%s durable=%s channel=%u "
              "heartbeat=%u sec",
              config->rabbitmq_host, config->rabbitmq_port,
              config->rabbitmq_vhost, config->rabbitmq_queue,
              config->rabbitmq_queue_durable ? "true" : "false",
              (unsigned)config->rabbitmq_channel,
              (unsigned)config->rabbitmq_heartbeat_seconds);
  } else {
    ulog_info(
        "RabbitMQ publishing disabled by config (rabbitmq.enabled=false)");
  }
}

[[nodiscard]] bool app_runtime_run(const app_config *config) {
  if (config == nullptr || !app_install_signal_handlers()) {
    return false;
  }

  app_log_startup_config(config);

  ws_subscriber_redis_config_t redis_config = {
      .host = config->redis_host,
      .port = config->redis_port,
      .monitored_set_key = config->redis_monitored_set_key,
  };
  ws_rabbitmq_config_t rabbitmq_config = {
      .host = config->rabbitmq_host,
      .port = config->rabbitmq_port,
      .username = config->rabbitmq_username,
      .password = config->rabbitmq_password,
      .vhost = config->rabbitmq_vhost,
      .queue = config->rabbitmq_queue,
      .queue_durable = config->rabbitmq_queue_durable,
      .channel = config->rabbitmq_channel,
      .heartbeat_seconds = config->rabbitmq_heartbeat_seconds,
  };

  ws_subscriber_set_stop_check(app_is_shutdown_requested);
  uint32_t reconnect_backoff_ms = config->reconnect_initial_backoff_ms;
  size_t reconnect_attempt = 0;
  bool ok = false;

  /* A successful run or an explicit shutdown terminates the reconnect loop. */
  while (true) {
    ok = ws_subscriber_run_ex_with_integrations_and_timeouts(
        config->host, config->port, config->path, config->request,
        config->secure, &redis_config,
        config->rabbitmq_enabled ? &rabbitmq_config : nullptr,
        config->read_timeout_seconds, config->write_timeout_seconds);
    if (ok || app_is_shutdown_requested() || !config->reconnect_enabled) {
      break;
    }

    ++reconnect_attempt;
    ulog_warn(
        "Subscriber exited unexpectedly, reconnecting (attempt=%zu) in %u ms",
        reconnect_attempt, (unsigned)reconnect_backoff_ms);
    if (!app_sleep_milliseconds(reconnect_backoff_ms)) {
      break;
    }

    /* Saturating exponential backoff avoids overflow and reconnect storms. */
    if (reconnect_backoff_ms < config->reconnect_max_backoff_ms) {
      uint64_t next_backoff = (uint64_t)reconnect_backoff_ms * 2U;
      reconnect_backoff_ms = (next_backoff > config->reconnect_max_backoff_ms)
                                 ? config->reconnect_max_backoff_ms
                                 : (uint32_t)next_backoff;
    }
  }
  ws_subscriber_set_stop_check(nullptr);

  if (app_is_shutdown_requested()) {
    ulog_info("Received %s, shutting down gracefully",
              app_signal_name((int)app_shutdown_signal));
    return true;
  }

  return ok;
}
