#include "websocket-client/monitor_config.h"

#include "parg/parg.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

#include <errno.h>
#include <limits.h>
#include <stdckdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/*
 * Untrusted CLI and TOML input is validated before state mutation. String
 * values are copied into explicit owned fields; RabbitMQ credentials are never
 * written to logs by this module.
 */

static constexpr char APP_DEFAULT_CONFIG_PATH[] = "conf/config.toml";
static constexpr char APP_DEFAULT_HOST[] =
    "ethereum-sepolia-rpc.publicnode.com";
static constexpr uint16_t APP_DEFAULT_PORT = 443;
static constexpr char APP_DEFAULT_PATH[] = "/";
static constexpr ulog_level APP_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
static constexpr bool APP_DEFAULT_LOG_COLOR = true;
static constexpr char APP_DEFAULT_REQUEST[] =
    "{\n"
    "  \"jsonrpc\": \"2.0\",\n"
    "  \"id\": 1,\n"
    "  \"method\": \"eth_subscribe\",\n"
    "  \"params\": [\"newPendingTransactions\", true]\n"
    "}";
static constexpr char APP_DEFAULT_REDIS_HOST[] = "127.0.0.1";
static constexpr uint16_t APP_DEFAULT_REDIS_PORT = 6379;
static constexpr char APP_DEFAULT_REDIS_MONITORED_SET_KEY[] =
    "monitored_addresses";
static constexpr char APP_DEFAULT_RABBITMQ_HOST[] = "127.0.0.1";
static constexpr uint16_t APP_DEFAULT_RABBITMQ_PORT = 5672;
static constexpr char APP_DEFAULT_RABBITMQ_USERNAME[] = "guest";
static constexpr char APP_DEFAULT_RABBITMQ_PASSWORD[] = "guest";
static constexpr char APP_DEFAULT_RABBITMQ_VHOST[] = "/";
static constexpr char APP_DEFAULT_RABBITMQ_QUEUE[] = "monitored_transactions";
static constexpr uint16_t APP_DEFAULT_RABBITMQ_CHANNEL = 1;
static constexpr uint16_t APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS = 30;
static constexpr uint32_t APP_DEFAULT_READ_TIMEOUT_SECONDS = 5;
static constexpr uint32_t APP_DEFAULT_WRITE_TIMEOUT_SECONDS = 5;
static constexpr uint32_t APP_DEFAULT_RECONNECT_INITIAL_BACKOFF_MS = 1'000;
static constexpr uint32_t APP_DEFAULT_RECONNECT_MAX_BACKOFF_MS = 30'000;
static constexpr int APP_OPTION_REDIS_HOST = 1000;
static constexpr int APP_OPTION_REDIS_PORT = 1001;
static constexpr int APP_OPTION_REDIS_KEY = 1002;
static constexpr int APP_OPTION_RABBITMQ_HOST = 1003;
static constexpr int APP_OPTION_RABBITMQ_PORT = 1004;
static constexpr int APP_OPTION_RABBITMQ_USER = 1005;
static constexpr int APP_OPTION_RABBITMQ_PASSWORD = 1006;
static constexpr int APP_OPTION_RABBITMQ_VHOST = 1007;
static constexpr int APP_OPTION_RABBITMQ_QUEUE = 1008;
static constexpr int APP_OPTION_RABBITMQ_QUEUE_DURABLE = 1009;
static constexpr int APP_OPTION_RABBITMQ_QUEUE_TRANSIENT = 1010;
void app_print_usage(const char *program_name) {
  printf("Usage: %s [options] [host] [port] [path] [request]\n", program_name);
  printf("\n");
  printf("Options:\n");
  printf("  -c, --config <file>    TOML config file path (default: %s)\n",
         APP_DEFAULT_CONFIG_PATH);
  printf("  -H, --host <host>      WebSocket host\n");
  printf("  -p, --port <port>      WebSocket port (1..65535)\n");
  printf("  -P, --path <path>      WebSocket path\n");
  printf("  -r, --request <json>   JSON-RPC subscription request body\n");
  printf("      --redis-host <host> Redis host\n");
  printf("      --redis-port <port> Redis port (1..65535)\n");
  printf("      --redis-key <key>   Redis set key for monitored addresses\n");
  printf("      --rabbitmq-host <host> RabbitMQ host\n");
  printf("      --rabbitmq-port <port> RabbitMQ port (1..65535)\n");
  printf("      --rabbitmq-user <user> RabbitMQ username\n");
  printf("      --rabbitmq-password <pass> RabbitMQ password\n");
  printf("      --rabbitmq-vhost <vhost> RabbitMQ virtual host\n");
  printf("      --rabbitmq-queue <name> RabbitMQ queue name\n");
  printf("      --rabbitmq-queue-durable  Declare queue as durable\n");
  printf("      --rabbitmq-queue-transient Declare queue as transient\n");
  printf("  -s, --secure           Use wss:// (TLS)\n");
  printf("  -i, --insecure         Use ws:// (plain TCP)\n");
  printf("  -h, --help             Show this help message\n");
  printf("\n");
  printf("TOML keys:\n");
  printf("  connection.host\n");
  printf("  connection.port\n");
  printf("  connection.path\n");
  printf("  connection.secure\n");
  printf("  connection.read_timeout_seconds\n");
  printf("  connection.write_timeout_seconds\n");
  printf("  subscription.request\n");
  printf("  retry.enabled\n");
  printf("  retry.initial_backoff_ms\n");
  printf("  retry.max_backoff_ms\n");
  printf("  redis.host\n");
  printf("  redis.port\n");
  printf("  redis.monitored_set_key\n");
  printf("  rabbitmq.host\n");
  printf("  rabbitmq.port\n");
  printf("  rabbitmq.username\n");
  printf("  rabbitmq.password\n");
  printf("  rabbitmq.vhost\n");
  printf("  rabbitmq.queue\n");
  printf("  rabbitmq.queue_durable\n");
  printf("  rabbitmq.channel\n");
  printf("  rabbitmq.heartbeat_seconds\n");
  printf("  rabbitmq.enabled\n");
  printf("  logging.level (trace|debug|info|warn|error|fatal)\n");
  printf("  logging.color (true|false)\n");
}
[[nodiscard]] static char *app_string_duplicate(const char *text) {
  if (text == nullptr) {
    return nullptr;
  }

  auto len = strlen(text);
  size_t allocation_length = 0;
  if (ckd_add(&allocation_length, len, (size_t)1)) {
    return nullptr;
  }

  auto copy = calloc(allocation_length, sizeof(char));
  if (copy == nullptr) {
    return nullptr;
  }
  memcpy(copy, text, len + 1);
  return copy;
}

[[nodiscard]] static bool app_replace_string(const char **target, char **owned,
                                             const char *value) {
  if (value == nullptr) {
    return false;
  }

  char *copy = app_string_duplicate(value);
  if (copy == nullptr) {
    return false;
  }

  free(*owned);
  *owned = copy;
  *target = copy;
  return true;
}

void app_config_set_defaults(monitor_config_t *config) {
  config->host = APP_DEFAULT_HOST;
  config->port = (app_port_t){.value = APP_DEFAULT_PORT};
  config->path = APP_DEFAULT_PATH;
  config->request = APP_DEFAULT_REQUEST;
  config->log_level = APP_DEFAULT_LOG_LEVEL;
  config->log_color = APP_DEFAULT_LOG_COLOR;
  config->secure = true;
  config->redis_host = APP_DEFAULT_REDIS_HOST;
  config->redis_port = (app_port_t){.value = APP_DEFAULT_REDIS_PORT};
  config->redis_monitored_set_key = APP_DEFAULT_REDIS_MONITORED_SET_KEY;
  config->rabbitmq_host = APP_DEFAULT_RABBITMQ_HOST;
  config->rabbitmq_port = (app_port_t){.value = APP_DEFAULT_RABBITMQ_PORT};
  config->rabbitmq_username = APP_DEFAULT_RABBITMQ_USERNAME;
  config->rabbitmq_password = APP_DEFAULT_RABBITMQ_PASSWORD;
  config->rabbitmq_vhost = APP_DEFAULT_RABBITMQ_VHOST;
  config->rabbitmq_queue = APP_DEFAULT_RABBITMQ_QUEUE;
  config->rabbitmq_queue_durable = true;
  config->rabbitmq_channel =
      (app_rabbitmq_channel_t){.value = APP_DEFAULT_RABBITMQ_CHANNEL};
  config->rabbitmq_heartbeat =
      (app_seconds_t){.value = APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS};
  config->rabbitmq_enabled = true;
  config->read_timeout =
      (app_seconds_t){.value = APP_DEFAULT_READ_TIMEOUT_SECONDS};
  config->write_timeout =
      (app_seconds_t){.value = APP_DEFAULT_WRITE_TIMEOUT_SECONDS};
  config->reconnect_enabled = true;
  config->reconnect_initial_backoff =
      (app_milliseconds_t){.value = APP_DEFAULT_RECONNECT_INITIAL_BACKOFF_MS};
  config->reconnect_max_backoff =
      (app_milliseconds_t){.value = APP_DEFAULT_RECONNECT_MAX_BACKOFF_MS};
}

[[nodiscard]] static bool app_parse_log_level(const char *value,
                                              ulog_level *out_level) {
  if (value == nullptr || out_level == nullptr) {
    return false;
  }

  if (strcasecmp(value, "trace") == 0) {
    *out_level = ULOG_LEVEL_TRACE;
    return true;
  }
  if (strcasecmp(value, "debug") == 0) {
    *out_level = ULOG_LEVEL_DEBUG;
    return true;
  }
  if (strcasecmp(value, "info") == 0) {
    *out_level = ULOG_LEVEL_INFO;
    return true;
  }
  if (strcasecmp(value, "warn") == 0 || strcasecmp(value, "warning") == 0) {
    *out_level = ULOG_LEVEL_WARN;
    return true;
  }
  if (strcasecmp(value, "error") == 0) {
    *out_level = ULOG_LEVEL_ERROR;
    return true;
  }
  if (strcasecmp(value, "fatal") == 0) {
    *out_level = ULOG_LEVEL_FATAL;
    return true;
  }

  return false;
}

[[nodiscard]] bool app_apply_log_level(ulog_level level) {
  auto status = ulog_output_level_set_all(level);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log level '%s' (status=%d)\n",
          ulog_level_to_string(level), (int)status);
  return false;
}

[[nodiscard]] bool app_apply_log_color(bool enabled) {
  auto status = ulog_color_config(enabled);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log color '%s' (status=%d)\n",
          enabled ? "true" : "false", (int)status);
  return false;
}

[[nodiscard]] bool app_apply_log_style_defaults() {
  auto status = ulog_prefix_config(false);
  if (status != ULOG_STATUS_OK && status != ULOG_STATUS_DISABLED) {
    fprintf(stderr, "Failed to disable log prefix (status=%d)\n", (int)status);
    return false;
  }

  status = ulog_time_config(false);
  if (status != ULOG_STATUS_OK && status != ULOG_STATUS_DISABLED) {
    fprintf(stderr, "Failed to disable log timestamps (status=%d)\n",
            (int)status);
    return false;
  }

  return true;
}

static void app_clear_and_free_secret(char **secret) {
  if (secret == nullptr || *secret == nullptr) {
    return;
  }

  auto length = strlen(*secret);
  volatile unsigned char *bytes = (volatile unsigned char *)*secret;
  for (size_t index = 0; index < length; ++index) {
    bytes[index] = 0;
  }
  free(*secret);
  *secret = nullptr;
}

void app_config_cleanup(monitor_config_t *config) {
  free(config->host_owned);
  free(config->path_owned);
  free(config->request_owned);
  free(config->redis_host_owned);
  free(config->redis_monitored_set_key_owned);
  free(config->rabbitmq_host_owned);
  free(config->rabbitmq_username_owned);
  app_clear_and_free_secret(&config->rabbitmq_password_owned);
  free(config->rabbitmq_vhost_owned);
  free(config->rabbitmq_queue_owned);
  config->host_owned = nullptr;
  config->path_owned = nullptr;
  config->request_owned = nullptr;
  config->redis_host_owned = nullptr;
  config->redis_monitored_set_key_owned = nullptr;
  config->rabbitmq_host_owned = nullptr;
  config->rabbitmq_username_owned = nullptr;
  config->rabbitmq_vhost_owned = nullptr;
  config->rabbitmq_queue_owned = nullptr;
}

[[nodiscard]] static bool app_parse_port(const char *value,
                                         uint16_t *out_port) {
  if (value == nullptr || value[0] == '\0' || value[0] == '-') {
    return false;
  }

  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed == 0 ||
      parsed > UINT16_MAX) {
    return false;
  }

  *out_port = (uint16_t)parsed;
  return true;
}

[[nodiscard]] static bool
app_apply_toml_uint32(toml_datum_t root, const char *key, uint32_t min_value,
                      uint32_t max_value, uint32_t *target) {
  if (root.type != TOML_TABLE || key == nullptr || target == nullptr ||
      min_value > max_value) {
    return false;
  }

  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }

  if (value.type != TOML_INT64 || value.u.int64 < (int64_t)min_value ||
      value.u.int64 > (int64_t)max_value) {
    ulog_error("Config key '%s' must be an integer in range %u..%u", key,
               (unsigned)min_value, (unsigned)max_value);
    return false;
  }

  *target = (uint32_t)value.u.int64;
  return true;
}

[[nodiscard]] static bool app_apply_toml_string(toml_datum_t root,
                                                const char *key,
                                                const char **target,
                                                char **owned) {
  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }
  if (value.type != TOML_STRING) {
    ulog_error("Config key '%s' must be a string", key);
    return false;
  }
  if (!app_replace_string(target, owned, value.u.s)) {
    ulog_error("Out of memory while loading config key '%s'", key);
    return false;
  }
  return true;
}

/* Load the optional TOML layer over defaults; no CLI values are applied here.
 */
[[nodiscard]] static bool
app_load_toml_config_impl(monitor_config_t *config,
                          const monitor_cli_overrides_t *overrides) {
  FILE *fp = fopen(overrides->config_path, "rb");
  if (fp == nullptr) {
    if (!overrides->config_path_set && errno == ENOENT) {
      ulog_debug("Config file '%s' not found, using defaults",
                 overrides->config_path);
      return true;
    }
    ulog_error("Failed to open config file '%s': %s", overrides->config_path,
               strerror(errno));
    return false;
  }

  toml_result_t parsed = toml_parse_file(fp);
  if (fclose(fp) != 0) {
    ulog_warn("Failed to close config file '%s'", overrides->config_path);
  }
  if (!parsed.ok) {
    ulog_error("Failed to parse config file '%s': %s", overrides->config_path,
               parsed.errmsg);
    toml_free(parsed);
    return false;
  }

  bool ok = true;

  ok = ok && app_apply_toml_string(parsed.toptab, "connection.host",
                                   &config->host, &config->host_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "connection.path",
                                   &config->path, &config->path_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "subscription.request",
                                   &config->request, &config->request_owned);
  ok = ok &&
       app_apply_toml_string(parsed.toptab, "redis.host", &config->redis_host,
                             &config->redis_host_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "redis.monitored_set_key",
                                   &config->redis_monitored_set_key,
                                   &config->redis_monitored_set_key_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "rabbitmq.host",
                                   &config->rabbitmq_host,
                                   &config->rabbitmq_host_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "rabbitmq.username",
                                   &config->rabbitmq_username,
                                   &config->rabbitmq_username_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "rabbitmq.password",
                                   &config->rabbitmq_password,
                                   &config->rabbitmq_password_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "rabbitmq.vhost",
                                   &config->rabbitmq_vhost,
                                   &config->rabbitmq_vhost_owned);
  ok = ok && app_apply_toml_string(parsed.toptab, "rabbitmq.queue",
                                   &config->rabbitmq_queue,
                                   &config->rabbitmq_queue_owned);

  toml_datum_t port = toml_seek(parsed.toptab, "connection.port");
  if (ok && port.type != TOML_UNKNOWN) {
    if (port.type != TOML_INT64 || port.u.int64 <= 0 ||
        port.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'connection.port' must be an integer in range 1..65535");
      ok = false;
    } else {
      config->port.value = (uint16_t)port.u.int64;
    }
  }

  toml_datum_t secure = toml_seek(parsed.toptab, "connection.secure");
  if (ok && secure.type != TOML_UNKNOWN) {
    if (secure.type != TOML_BOOLEAN) {
      ulog_error("Config key 'connection.secure' must be a boolean");
      ok = false;
    } else {
      config->secure = secure.u.boolean;
    }
  }

  ok = ok &&
       app_apply_toml_uint32(parsed.toptab, "connection.read_timeout_seconds",
                             1, (uint32_t)INT_MAX, &config->read_timeout.value);
  ok = ok && app_apply_toml_uint32(
                 parsed.toptab, "connection.write_timeout_seconds", 1,
                 (uint32_t)INT_MAX, &config->write_timeout.value);

  toml_datum_t reconnect_enabled = toml_seek(parsed.toptab, "retry.enabled");
  if (ok && reconnect_enabled.type != TOML_UNKNOWN) {
    if (reconnect_enabled.type != TOML_BOOLEAN) {
      ulog_error("Config key 'retry.enabled' must be a boolean");
      ok = false;
    } else {
      config->reconnect_enabled = reconnect_enabled.u.boolean;
    }
  }
  ok = ok && app_apply_toml_uint32(parsed.toptab, "retry.initial_backoff_ms", 1,
                                   (uint32_t)INT_MAX,
                                   &config->reconnect_initial_backoff.value);
  ok = ok && app_apply_toml_uint32(parsed.toptab, "retry.max_backoff_ms", 1,
                                   (uint32_t)INT_MAX,
                                   &config->reconnect_max_backoff.value);
  if (ok && config->reconnect_initial_backoff.value >
                config->reconnect_max_backoff.value) {
    ulog_error("Config key 'retry.max_backoff_ms' must be greater than or "
               "equal to retry.initial_backoff_ms");
    ok = false;
  }

  toml_datum_t log_level = toml_seek(parsed.toptab, "logging.level");
  if (ok && log_level.type != TOML_UNKNOWN) {
    if (log_level.type != TOML_STRING) {
      ulog_error("Config key 'logging.level' must be a string");
      ok = false;
    } else if (!app_parse_log_level(log_level.u.s, &config->log_level)) {
      ulog_error("Config key 'logging.level' has invalid value '%s' (expected "
                 "one of: trace, debug, info, warn, error, fatal)",
                 log_level.u.s);
      ok = false;
    }
  }

  toml_datum_t log_color = toml_seek(parsed.toptab, "logging.color");
  if (ok && log_color.type != TOML_UNKNOWN) {
    if (log_color.type != TOML_BOOLEAN) {
      ulog_error("Config key 'logging.color' must be a boolean");
      ok = false;
    } else {
      config->log_color = log_color.u.boolean;
    }
  }

  toml_datum_t redis_port = toml_seek(parsed.toptab, "redis.port");
  if (ok && redis_port.type != TOML_UNKNOWN) {
    if (redis_port.type != TOML_INT64 || redis_port.u.int64 <= 0 ||
        redis_port.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'redis.port' must be an integer in range 1..65535");
      ok = false;
    } else {
      config->redis_port.value = (uint16_t)redis_port.u.int64;
    }
  }

  toml_datum_t rabbitmq_port = toml_seek(parsed.toptab, "rabbitmq.port");
  if (ok && rabbitmq_port.type != TOML_UNKNOWN) {
    if (rabbitmq_port.type != TOML_INT64 || rabbitmq_port.u.int64 <= 0 ||
        rabbitmq_port.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'rabbitmq.port' must be an integer in range 1..65535");
      ok = false;
    } else {
      config->rabbitmq_port.value = (uint16_t)rabbitmq_port.u.int64;
    }
  }

  toml_datum_t rabbitmq_queue_durable =
      toml_seek(parsed.toptab, "rabbitmq.queue_durable");
  if (ok && rabbitmq_queue_durable.type != TOML_UNKNOWN) {
    if (rabbitmq_queue_durable.type != TOML_BOOLEAN) {
      ulog_error("Config key 'rabbitmq.queue_durable' must be a boolean");
      ok = false;
    } else {
      config->rabbitmq_queue_durable = rabbitmq_queue_durable.u.boolean;
    }
  }

  toml_datum_t rabbitmq_enabled = toml_seek(parsed.toptab, "rabbitmq.enabled");
  if (ok && rabbitmq_enabled.type != TOML_UNKNOWN) {
    if (rabbitmq_enabled.type != TOML_BOOLEAN) {
      ulog_error("Config key 'rabbitmq.enabled' must be a boolean");
      ok = false;
    } else {
      config->rabbitmq_enabled = rabbitmq_enabled.u.boolean;
    }
  }

  toml_datum_t rabbitmq_channel = toml_seek(parsed.toptab, "rabbitmq.channel");
  if (ok && rabbitmq_channel.type != TOML_UNKNOWN) {
    if (rabbitmq_channel.type != TOML_INT64 || rabbitmq_channel.u.int64 <= 0 ||
        rabbitmq_channel.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'rabbitmq.channel' must be an integer in range 1..65535");
      ok = false;
    } else {
      config->rabbitmq_channel.value = (uint16_t)rabbitmq_channel.u.int64;
    }
  }

  toml_datum_t rabbitmq_heartbeat_seconds =
      toml_seek(parsed.toptab, "rabbitmq.heartbeat_seconds");
  if (ok && rabbitmq_heartbeat_seconds.type != TOML_UNKNOWN) {
    if (rabbitmq_heartbeat_seconds.type != TOML_INT64 ||
        rabbitmq_heartbeat_seconds.u.int64 <= 0 ||
        rabbitmq_heartbeat_seconds.u.int64 > UINT16_MAX) {
      ulog_error("Config key 'rabbitmq.heartbeat_seconds' must be an integer "
                 "in range 1..65535");
      ok = false;
    } else {
      config->rabbitmq_heartbeat.value =
          (uint16_t)rabbitmq_heartbeat_seconds.u.int64;
    }
  }

  toml_free(parsed);
  return ok;
}

[[nodiscard]] app_config_status_t
app_load_toml_config(monitor_config_t *config,
                     const monitor_cli_overrides_t *overrides) {
  return app_load_toml_config_impl(config, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_LOAD_ERROR;
}

/* Parse argv into borrowed views so validation and mutation stay separate. */
[[nodiscard]] static bool
app_parse_cli_impl(int argc, char *argv[], monitor_cli_overrides_t *overrides) {
  *overrides = (monitor_cli_overrides_t){0};
  overrides->config_path = APP_DEFAULT_CONFIG_PATH;

  static const struct parg_option long_options[] = {
      {"config", PARG_REQARG, nullptr, 'c'},
      {"host", PARG_REQARG, nullptr, 'H'},
      {"port", PARG_REQARG, nullptr, 'p'},
      {"path", PARG_REQARG, nullptr, 'P'},
      {"request", PARG_REQARG, nullptr, 'r'},
      {"redis-host", PARG_REQARG, nullptr, APP_OPTION_REDIS_HOST},
      {"redis-port", PARG_REQARG, nullptr, APP_OPTION_REDIS_PORT},
      {"redis-key", PARG_REQARG, nullptr, APP_OPTION_REDIS_KEY},
      {"rabbitmq-host", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_HOST},
      {"rabbitmq-port", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_PORT},
      {"rabbitmq-user", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_USER},
      {"rabbitmq-password", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_PASSWORD},
      {"rabbitmq-vhost", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_VHOST},
      {"rabbitmq-queue", PARG_REQARG, nullptr, APP_OPTION_RABBITMQ_QUEUE},
      {"rabbitmq-queue-durable", PARG_NOARG, nullptr,
       APP_OPTION_RABBITMQ_QUEUE_DURABLE},
      {"rabbitmq-queue-transient", PARG_NOARG, nullptr,
       APP_OPTION_RABBITMQ_QUEUE_TRANSIENT},
      {"secure", PARG_NOARG, nullptr, 's'},
      {"insecure", PARG_NOARG, nullptr, 'i'},
      {"help", PARG_NOARG, nullptr, 'h'},
      {nullptr, PARG_NOARG, nullptr, 0},
  };

  const char *positionals[4] = {nullptr};
  int positional_count = 0;

  struct parg_state state;
  parg_init(&state);

  while (true) {
    int option = parg_getopt_long(&state, argc, argv, ":c:H:p:P:r:sih",
                                  long_options, nullptr);
    if (option == -1) {
      break;
    }

    switch (option) {
    case 1: {
      if (positional_count >=
          (int)(sizeof(positionals) / sizeof(positionals[0]))) {
        ulog_error("Too many positional arguments");
        return false;
      }
      positionals[positional_count++] = state.optarg;
      break;
    }
    case 'c':
      overrides->config_path = state.optarg;
      overrides->config_path_set = true;
      break;
    case 'H':
      overrides->host = state.optarg;
      break;
    case 'p':
      overrides->port_text = state.optarg;
      break;
    case 'P':
      overrides->path = state.optarg;
      break;
    case 'r':
      overrides->request = state.optarg;
      break;
    case APP_OPTION_REDIS_HOST:
      overrides->redis_host = state.optarg;
      break;
    case APP_OPTION_REDIS_PORT:
      overrides->redis_port_text = state.optarg;
      break;
    case APP_OPTION_REDIS_KEY:
      overrides->redis_monitored_set_key = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_HOST:
      overrides->rabbitmq_host = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_PORT:
      overrides->rabbitmq_port_text = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_USER:
      overrides->rabbitmq_username = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_PASSWORD:
      overrides->rabbitmq_password = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_VHOST:
      overrides->rabbitmq_vhost = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_QUEUE:
      overrides->rabbitmq_queue = state.optarg;
      break;
    case APP_OPTION_RABBITMQ_QUEUE_DURABLE:
      overrides->rabbitmq_queue_durable_set = true;
      overrides->rabbitmq_queue_durable = true;
      break;
    case APP_OPTION_RABBITMQ_QUEUE_TRANSIENT:
      overrides->rabbitmq_queue_durable_set = true;
      overrides->rabbitmq_queue_durable = false;
      break;
    case 's':
      overrides->secure_set = true;
      overrides->secure = true;
      break;
    case 'i':
      overrides->secure_set = true;
      overrides->secure = false;
      break;
    case 'h':
      overrides->show_help = true;
      break;
    case ':':
      ulog_error("Option '-%c' requires an argument", state.optopt);
      return false;
    case '?':
      if (state.optopt != 0) {
        ulog_error("Unrecognized option '-%c'", state.optopt);
      } else {
        ulog_error("Unrecognized or ambiguous long option");
      }
      return false;
    default:
      ulog_error("Unhandled option code: %d", option);
      return false;
    }
  }

  if (positional_count > 0 && overrides->host == nullptr) {
    overrides->host = positionals[0];
  }
  if (positional_count > 1 && overrides->port_text == nullptr) {
    overrides->port_text = positionals[1];
  }
  if (positional_count > 2 && overrides->path == nullptr) {
    overrides->path = positionals[2];
  }
  if (positional_count > 3 && overrides->request == nullptr) {
    overrides->request = positionals[3];
  }

  return true;
}

[[nodiscard]] app_config_status_t
app_parse_cli(int argc, char *argv[], monitor_cli_overrides_t *overrides) {
  return app_parse_cli_impl(argc, argv, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_INVALID_ARGUMENT;
}

/* Apply the highest-precedence configuration layer after full validation. */
[[nodiscard]] static bool
app_apply_cli_overrides_impl(monitor_config_t *config,
                             const monitor_cli_overrides_t *overrides) {
  if (overrides->host != nullptr &&
      !app_replace_string(&config->host, &config->host_owned,
                          overrides->host)) {
    ulog_error("Out of memory while applying --host");
    return false;
  }

  if (overrides->path != nullptr &&
      !app_replace_string(&config->path, &config->path_owned,
                          overrides->path)) {
    ulog_error("Out of memory while applying --path");
    return false;
  }

  if (overrides->request != nullptr &&
      !app_replace_string(&config->request, &config->request_owned,
                          overrides->request)) {
    ulog_error("Out of memory while applying --request");
    return false;
  }

  if (overrides->redis_host != nullptr &&
      !app_replace_string(&config->redis_host, &config->redis_host_owned,
                          overrides->redis_host)) {
    ulog_error("Out of memory while applying --redis-host");
    return false;
  }

  if (overrides->redis_monitored_set_key != nullptr &&
      !app_replace_string(&config->redis_monitored_set_key,
                          &config->redis_monitored_set_key_owned,
                          overrides->redis_monitored_set_key)) {
    ulog_error("Out of memory while applying --redis-key");
    return false;
  }

  if (overrides->rabbitmq_host != nullptr &&
      !app_replace_string(&config->rabbitmq_host, &config->rabbitmq_host_owned,
                          overrides->rabbitmq_host)) {
    ulog_error("Out of memory while applying --rabbitmq-host");
    return false;
  }

  if (overrides->rabbitmq_username != nullptr &&
      !app_replace_string(&config->rabbitmq_username,
                          &config->rabbitmq_username_owned,
                          overrides->rabbitmq_username)) {
    ulog_error("Out of memory while applying --rabbitmq-user");
    return false;
  }

  if (overrides->rabbitmq_password != nullptr &&
      !app_replace_string(&config->rabbitmq_password,
                          &config->rabbitmq_password_owned,
                          overrides->rabbitmq_password)) {
    ulog_error("Out of memory while applying --rabbitmq-password");
    return false;
  }

  if (overrides->rabbitmq_vhost != nullptr &&
      !app_replace_string(&config->rabbitmq_vhost,
                          &config->rabbitmq_vhost_owned,
                          overrides->rabbitmq_vhost)) {
    ulog_error("Out of memory while applying --rabbitmq-vhost");
    return false;
  }

  if (overrides->rabbitmq_queue != nullptr &&
      !app_replace_string(&config->rabbitmq_queue,
                          &config->rabbitmq_queue_owned,
                          overrides->rabbitmq_queue)) {
    ulog_error("Out of memory while applying --rabbitmq-queue");
    return false;
  }

  if (overrides->port_text != nullptr) {
    uint16_t port = 0;
    if (!app_parse_port(overrides->port_text, &port)) {
      ulog_error("Invalid port '%s' (expected integer in range 1..65535)",
                 overrides->port_text);
      return false;
    }
    config->port.value = port;
  }

  if (overrides->secure_set) {
    config->secure = overrides->secure;
  }

  if (overrides->redis_port_text != nullptr) {
    uint16_t redis_port = 0;
    if (!app_parse_port(overrides->redis_port_text, &redis_port)) {
      ulog_error("Invalid redis port '%s' (expected integer in range 1..65535)",
                 overrides->redis_port_text);
      return false;
    }
    config->redis_port.value = redis_port;
  }

  if (overrides->rabbitmq_port_text != nullptr) {
    uint16_t rabbitmq_port = 0;
    if (!app_parse_port(overrides->rabbitmq_port_text, &rabbitmq_port)) {
      ulog_error(
          "Invalid rabbitmq port '%s' (expected integer in range 1..65535)",
          overrides->rabbitmq_port_text);
      return false;
    }
    config->rabbitmq_port.value = rabbitmq_port;
  }

  if (overrides->rabbitmq_queue_durable_set) {
    config->rabbitmq_queue_durable = overrides->rabbitmq_queue_durable;
  }

  return true;
}

[[nodiscard]] app_config_status_t
app_apply_cli_overrides(monitor_config_t *config,
                        const monitor_cli_overrides_t *overrides) {
  return app_apply_cli_overrides_impl(config, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_INVALID_VALUE;
}
