#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdckdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include "websocket-client/subscriber.h"

#include "parg/parg.h"
#include "parson/parson.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

#if defined(USE_MIMALLOC)
#include <mimalloc.h>
#endif

typedef struct app_config app_config;
struct app_config {
  const char *host;
  uint16_t port;
  const char *path;
  const char *request;
  ulog_level log_level;
  bool log_color;
  bool secure;
  const char *redis_host;
  uint16_t redis_port;
  const char *redis_monitored_set_key;
  const char *rabbitmq_host;
  uint16_t rabbitmq_port;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;
  bool rabbitmq_queue_durable;
  uint16_t rabbitmq_channel;
  uint16_t rabbitmq_heartbeat_seconds;
  bool rabbitmq_enabled;
  uint32_t read_timeout_seconds;
  uint32_t write_timeout_seconds;
  bool reconnect_enabled;
  uint32_t reconnect_initial_backoff_ms;
  uint32_t reconnect_max_backoff_ms;

  char *host_owned;
  char *path_owned;
  char *request_owned;
  char *redis_host_owned;
  char *redis_monitored_set_key_owned;
  char *rabbitmq_host_owned;
  char *rabbitmq_username_owned;
  char *rabbitmq_password_owned;
  char *rabbitmq_vhost_owned;
  char *rabbitmq_queue_owned;
};

typedef struct app_cli_overrides app_cli_overrides;
struct app_cli_overrides {
  const char *config_path;
  bool config_path_set;

  const char *host;
  const char *port_text;
  const char *path;
  const char *request;
  const char *redis_host;
  const char *redis_port_text;
  const char *redis_monitored_set_key;
  const char *rabbitmq_host;
  const char *rabbitmq_port_text;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;

  bool secure_set;
  bool secure;
  bool rabbitmq_queue_durable_set;
  bool rabbitmq_queue_durable;
  bool show_help;
};

static constexpr char APP_DEFAULT_CONFIG_PATH[] = "config.toml";
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

static void app_print_usage(const char *program_name) {
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

static void app_configure_allocator_overrides() {
#if defined(USE_MIMALLOC)
  toml_option_t toml_options = toml_default_option();
  toml_options.mem_realloc = mi_realloc;
  toml_options.mem_free = mi_free;
  toml_set_option(toml_options);
  json_set_allocation_functions(mi_malloc, mi_free);
#endif
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

static void app_config_set_defaults(app_config *config) {
  config->host = APP_DEFAULT_HOST;
  config->port = APP_DEFAULT_PORT;
  config->path = APP_DEFAULT_PATH;
  config->request = APP_DEFAULT_REQUEST;
  config->log_level = APP_DEFAULT_LOG_LEVEL;
  config->log_color = APP_DEFAULT_LOG_COLOR;
  config->secure = true;
  config->redis_host = APP_DEFAULT_REDIS_HOST;
  config->redis_port = APP_DEFAULT_REDIS_PORT;
  config->redis_monitored_set_key = APP_DEFAULT_REDIS_MONITORED_SET_KEY;
  config->rabbitmq_host = APP_DEFAULT_RABBITMQ_HOST;
  config->rabbitmq_port = APP_DEFAULT_RABBITMQ_PORT;
  config->rabbitmq_username = APP_DEFAULT_RABBITMQ_USERNAME;
  config->rabbitmq_password = APP_DEFAULT_RABBITMQ_PASSWORD;
  config->rabbitmq_vhost = APP_DEFAULT_RABBITMQ_VHOST;
  config->rabbitmq_queue = APP_DEFAULT_RABBITMQ_QUEUE;
  config->rabbitmq_queue_durable = true;
  config->rabbitmq_channel = APP_DEFAULT_RABBITMQ_CHANNEL;
  config->rabbitmq_heartbeat_seconds = APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS;
  config->rabbitmq_enabled = true;
  config->read_timeout_seconds = APP_DEFAULT_READ_TIMEOUT_SECONDS;
  config->write_timeout_seconds = APP_DEFAULT_WRITE_TIMEOUT_SECONDS;
  config->reconnect_enabled = true;
  config->reconnect_initial_backoff_ms =
      APP_DEFAULT_RECONNECT_INITIAL_BACKOFF_MS;
  config->reconnect_max_backoff_ms = APP_DEFAULT_RECONNECT_MAX_BACKOFF_MS;
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

[[nodiscard]] static bool app_apply_log_level(ulog_level level) {
  auto status = ulog_output_level_set_all(level);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log level '%s' (status=%d)\n",
          ulog_level_to_string(level), (int)status);
  return false;
}

[[nodiscard]] static bool app_apply_log_color(bool enabled) {
  auto status = ulog_color_config(enabled);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log color '%s' (status=%d)\n",
          enabled ? "true" : "false", (int)status);
  return false;
}

[[nodiscard]] static bool app_apply_log_style_defaults() {
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

static void app_config_cleanup(app_config *config) {
  free(config->host_owned);
  free(config->path_owned);
  free(config->request_owned);
  free(config->redis_host_owned);
  free(config->redis_monitored_set_key_owned);
  free(config->rabbitmq_host_owned);
  free(config->rabbitmq_username_owned);
  free(config->rabbitmq_password_owned);
  free(config->rabbitmq_vhost_owned);
  free(config->rabbitmq_queue_owned);
  config->host_owned = nullptr;
  config->path_owned = nullptr;
  config->request_owned = nullptr;
  config->redis_host_owned = nullptr;
  config->redis_monitored_set_key_owned = nullptr;
  config->rabbitmq_host_owned = nullptr;
  config->rabbitmq_username_owned = nullptr;
  config->rabbitmq_password_owned = nullptr;
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

[[nodiscard]] static bool app_sleep_milliseconds(uint32_t duration_ms) {
  struct timespec request = {
      .tv_sec = (time_t)(duration_ms / 1000U),
      .tv_nsec = (long)((duration_ms % 1000U) * 1000000U),
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

[[nodiscard]] static bool
app_load_toml_config(app_config *config, const app_cli_overrides *overrides) {
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
      config->port = (uint16_t)port.u.int64;
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

  ok = ok && app_apply_toml_uint32(
                 parsed.toptab, "connection.read_timeout_seconds", 1,
                 (uint32_t)INT_MAX, &config->read_timeout_seconds);
  ok = ok && app_apply_toml_uint32(
                 parsed.toptab, "connection.write_timeout_seconds", 1,
                 (uint32_t)INT_MAX, &config->write_timeout_seconds);

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
                                   &config->reconnect_initial_backoff_ms);
  ok = ok && app_apply_toml_uint32(parsed.toptab, "retry.max_backoff_ms", 1,
                                   (uint32_t)INT_MAX,
                                   &config->reconnect_max_backoff_ms);
  if (ok &&
      config->reconnect_initial_backoff_ms > config->reconnect_max_backoff_ms) {
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
      config->redis_port = (uint16_t)redis_port.u.int64;
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
      config->rabbitmq_port = (uint16_t)rabbitmq_port.u.int64;
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
      config->rabbitmq_channel = (uint16_t)rabbitmq_channel.u.int64;
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
      config->rabbitmq_heartbeat_seconds =
          (uint16_t)rabbitmq_heartbeat_seconds.u.int64;
    }
  }

  toml_free(parsed);
  return ok;
}

[[nodiscard]] static bool app_parse_cli(int argc, char *argv[],
                                        app_cli_overrides *overrides) {
  *overrides = (app_cli_overrides){0};
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

[[nodiscard]] static bool
app_apply_cli_overrides(app_config *config,
                        const app_cli_overrides *overrides) {
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
    config->port = port;
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
    config->redis_port = redis_port;
  }

  if (overrides->rabbitmq_port_text != nullptr) {
    uint16_t rabbitmq_port = 0;
    if (!app_parse_port(overrides->rabbitmq_port_text, &rabbitmq_port)) {
      ulog_error(
          "Invalid rabbitmq port '%s' (expected integer in range 1..65535)",
          overrides->rabbitmq_port_text);
      return false;
    }
    config->rabbitmq_port = rabbitmq_port;
  }

  if (overrides->rabbitmq_queue_durable_set) {
    config->rabbitmq_queue_durable = overrides->rabbitmq_queue_durable;
  }

  return true;
}

int main(int argc, char *argv[]) {
  app_configure_allocator_overrides();

  if (!app_apply_log_style_defaults()) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  app_cli_overrides overrides = {0};
  if (!app_parse_cli(argc, argv, &overrides)) {
    app_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    app_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  app_config config = {0};
  app_config_set_defaults(&config);

  bool ok = app_load_toml_config(&config, &overrides);
  ok = ok && app_apply_cli_overrides(&config, &overrides);
  if (!ok) {
    app_config_cleanup(&config);
    return EXIT_FAILURE;
  }
  if (!app_apply_log_color(config.log_color)) {
    app_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  if (!app_apply_log_level(config.log_level)) {
    app_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  if (!app_install_signal_handlers()) {
    app_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  ulog_info("Starting websocket subscriber (log_level=%s color=%s)",
            ulog_level_to_string(config.log_level),
            config.log_color ? "true" : "false");
  ulog_info("WebSocket endpoint: %s://%s:%u%s", config.secure ? "wss" : "ws",
            config.host, config.port, config.path);
  ulog_info("WebSocket timeouts: read=%u sec write=%u sec",
            (unsigned)config.read_timeout_seconds,
            (unsigned)config.write_timeout_seconds);
  if (config.reconnect_enabled) {
    ulog_info("Reconnect policy: enabled (initial=%u ms, max=%u ms)",
              (unsigned)config.reconnect_initial_backoff_ms,
              (unsigned)config.reconnect_max_backoff_ms);
  } else {
    ulog_info("Reconnect policy: disabled");
  }
  ulog_info("Redis monitor: %s:%u key=%s", config.redis_host, config.redis_port,
            config.redis_monitored_set_key);
  if (config.rabbitmq_enabled) {
    ulog_info("RabbitMQ target: %s:%u vhost=%s queue=%s durable=%s channel=%u "
              "heartbeat=%u sec",
              config.rabbitmq_host, config.rabbitmq_port, config.rabbitmq_vhost,
              config.rabbitmq_queue,
              config.rabbitmq_queue_durable ? "true" : "false",
              (unsigned)config.rabbitmq_channel,
              (unsigned)config.rabbitmq_heartbeat_seconds);
  } else {
    ulog_info(
        "RabbitMQ publishing disabled by config (rabbitmq.enabled=false)");
  }

  ws_subscriber_redis_config_t redis_config = {
      .host = config.redis_host,
      .port = config.redis_port,
      .monitored_set_key = config.redis_monitored_set_key,
  };
  ws_rabbitmq_config_t rabbitmq_config = {
      .host = config.rabbitmq_host,
      .port = config.rabbitmq_port,
      .username = config.rabbitmq_username,
      .password = config.rabbitmq_password,
      .vhost = config.rabbitmq_vhost,
      .queue = config.rabbitmq_queue,
      .queue_durable = config.rabbitmq_queue_durable,
      .channel = config.rabbitmq_channel,
      .heartbeat_seconds = config.rabbitmq_heartbeat_seconds,
  };

  ws_subscriber_set_stop_check(app_is_shutdown_requested);
  uint32_t reconnect_backoff_ms = config.reconnect_initial_backoff_ms;
  size_t reconnect_attempt = 0;
  while (true) {
    ok = ws_subscriber_run_ex_with_integrations_and_timeouts(
        config.host, config.port, config.path, config.request, config.secure,
        &redis_config, config.rabbitmq_enabled ? &rabbitmq_config : nullptr,
        config.read_timeout_seconds, config.write_timeout_seconds);
    if (ok || app_is_shutdown_requested() || !config.reconnect_enabled) {
      break;
    }

    ++reconnect_attempt;
    ulog_warn(
        "Subscriber exited unexpectedly, reconnecting (attempt=%zu) in %u ms",
        reconnect_attempt, (unsigned)reconnect_backoff_ms);
    if (!app_sleep_milliseconds(reconnect_backoff_ms)) {
      break;
    }

    if (reconnect_backoff_ms < config.reconnect_max_backoff_ms) {
      uint64_t next_backoff = (uint64_t)reconnect_backoff_ms * 2U;
      reconnect_backoff_ms = (next_backoff > config.reconnect_max_backoff_ms)
                                 ? config.reconnect_max_backoff_ms
                                 : (uint32_t)next_backoff;
    }
  }
  ws_subscriber_set_stop_check(nullptr);
  app_config_cleanup(&config);

  if (app_is_shutdown_requested()) {
    ulog_info("Received %s, shutting down gracefully",
              app_signal_name((int)app_shutdown_signal));
    (void)ulog_cleanup();
    return EXIT_SUCCESS;
  }

  if (!ok) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  (void)ulog_cleanup();
  return EXIT_SUCCESS;
}
