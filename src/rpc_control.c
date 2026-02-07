#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <uv.h>

#include "hiredis/hiredis.h"
#include "jsonrpc/jsonrpc.h"
#include "jsonrpc/server.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

constexpr int32_t RPC_CONTROL_DEFAULT_PORT = 8080;
constexpr char RPC_CONTROL_DEFAULT_HOST[] = "127.0.0.1";
constexpr int32_t RPC_CONTROL_DEFAULT_BACKLOG = 4096;
constexpr char RPC_CONTROL_DEFAULT_CONFIG_PATH[] = "config.toml";
constexpr char RPC_CONTROL_DEFAULT_AUTH_TOKEN[] = "CHANGE_ME";
constexpr ulog_level RPC_CONTROL_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
constexpr bool RPC_CONTROL_DEFAULT_LOG_COLOR = true;
constexpr char RPC_CONTROL_DEFAULT_REDIS_HOST[] = "127.0.0.1";
constexpr uint16_t RPC_CONTROL_DEFAULT_REDIS_PORT = 6379;
constexpr char RPC_CONTROL_DEFAULT_REDIS_SET_KEY[] = "monitored_addresses";
constexpr size_t RPC_CONTROL_ADDRESS_CAPACITY = 128;
constexpr char RPC_CONTROL_NON_STRING[] = "<non-string>";

constexpr int32_t JSONRPC_ERR_INVALID_PARAMS = -32602;
constexpr int32_t JSONRPC_ERR_INTERNAL = -32603;
constexpr int32_t JSONRPC_ERR_SERVER = -32000;
constexpr int32_t JSONRPC_ERR_UNAUTHORIZED = -32001;

typedef struct rpc_control_config rpc_control_config_t;
struct rpc_control_config {
  const char *host;
  int32_t port;
  int32_t backlog;
  const char *auth_token;
  ulog_level log_level;
  bool log_color;
  const char *redis_host;
  uint16_t redis_port;
  const char *redis_set_key;
  char *host_owned;
  char *auth_token_owned;
  char *redis_host_owned;
  char *redis_set_key_owned;
};

typedef struct rpc_control_cli_overrides rpc_control_cli_overrides_t;
struct rpc_control_cli_overrides {
  const char *config_path;
  bool config_path_set;
  const char *host;
  const char *port_text;
  const char *backlog_text;
  const char *auth_token;
  const char *redis_host;
  const char *redis_port_text;
  const char *redis_set_key;
  bool show_help;
};

typedef struct rpc_control_runtime rpc_control_runtime_t;
struct rpc_control_runtime {
  redisContext *redis;
  const char *redis_set_key;
  const char *auth_token;
  jsonrpc_conn_t **authenticated_connections;
  size_t authenticated_connection_count;
  size_t authenticated_connection_capacity;
};

typedef struct rpc_control_address_input rpc_control_address_input_t;
struct rpc_control_address_input {
  const char *single;
  const JSON_Array *array;
};

static rpc_control_runtime_t g_runtime = {0};
static volatile sig_atomic_t g_shutdown_signal = 0;

static const char *const RPC_CONTROL_METHOD_NAMES[] = {
    "ping",           "auth",
    "health",         "methods",
    "monitor_add",    "add_address",
    "add_addresses",  "monitor_remove",
    "remove_address", "remove_addresses",
    "monitor_has",    "is_monitored",
    "monitor_count",  "monitor_list",
    "monitor_clear",
};

static void rpc_control_print_usage(const char *program_name) {
  printf("Usage: %s [options]\n", program_name);
  printf("\n");
  printf("Options:\n");
  printf(
      "  -c, --config <file>            TOML config file path (default: %s)\n",
      RPC_CONTROL_DEFAULT_CONFIG_PATH);
  printf(
      "      --listen-host <host>       JSON-RPC listen host (default: %s)\n",
      RPC_CONTROL_DEFAULT_HOST);
  printf(
      "  -p, --port <port>              JSON-RPC listen port (default: %" PRId32
      ")\n",
      RPC_CONTROL_DEFAULT_PORT);
  printf(
      "      --backlog <count>          TCP listen backlog (default: %" PRId32
      ")\n",
      RPC_CONTROL_DEFAULT_BACKLOG);
  printf(
      "      --auth-token <token>       Require token via auth RPC method\n");
  printf("  -H, --redis-host <host>        Redis host (default: %s)\n",
         RPC_CONTROL_DEFAULT_REDIS_HOST);
  printf("  -r, --redis-port <port>        Redis port (default: %u)\n",
         RPC_CONTROL_DEFAULT_REDIS_PORT);
  printf("  -k, --redis-key <key>          Redis monitored-address set key "
         "(default: %s)\n",
         RPC_CONTROL_DEFAULT_REDIS_SET_KEY);
  printf("  -h, --help                     Show this help message\n");
  printf("\n");
  printf("TOML keys:\n");
  printf("  rpc_control.host\n");
  printf("  rpc_control.port\n");
  printf("  rpc_control.backlog\n");
  printf("  rpc_control.auth_token\n");
  printf("  logging.level (trace|debug|info|warn|error|fatal)\n");
  printf("  logging.color\n");
  printf("  redis.host\n");
  printf("  redis.port\n");
  printf("  redis.monitored_set_key\n");
  printf("\n");
  printf("JSON-RPC methods:\n");
  for (size_t i = 0; i < sizeof(RPC_CONTROL_METHOD_NAMES) /
                             sizeof(RPC_CONTROL_METHOD_NAMES[0]);
       ++i) {
    printf("  %s\n", RPC_CONTROL_METHOD_NAMES[i]);
  }
}

[[nodiscard]] static char *rpc_control_string_duplicate(const char *text) {
  if (text == nullptr) {
    return nullptr;
  }

  auto len = strlen(text);
  auto copy = calloc(len + 1, sizeof(char));
  if (copy == nullptr) {
    return nullptr;
  }
  memcpy(copy, text, len + 1);
  return copy;
}

[[nodiscard]] static bool rpc_control_replace_string(const char **target,
                                                     char **owned,
                                                     const char *value) {
  if (target == nullptr || owned == nullptr || value == nullptr) {
    return false;
  }

  char *copy = rpc_control_string_duplicate(value);
  if (copy == nullptr) {
    return false;
  }

  free(*owned);
  *owned = copy;
  *target = copy;
  return true;
}

static void rpc_control_config_set_defaults(rpc_control_config_t *config) {
  if (config == nullptr) {
    return;
  }

  config->host = RPC_CONTROL_DEFAULT_HOST;
  config->port = RPC_CONTROL_DEFAULT_PORT;
  config->backlog = RPC_CONTROL_DEFAULT_BACKLOG;
  config->auth_token = RPC_CONTROL_DEFAULT_AUTH_TOKEN;
  config->log_level = RPC_CONTROL_DEFAULT_LOG_LEVEL;
  config->log_color = RPC_CONTROL_DEFAULT_LOG_COLOR;
  config->redis_host = RPC_CONTROL_DEFAULT_REDIS_HOST;
  config->redis_port = RPC_CONTROL_DEFAULT_REDIS_PORT;
  config->redis_set_key = RPC_CONTROL_DEFAULT_REDIS_SET_KEY;
}

static void rpc_control_config_cleanup(rpc_control_config_t *config) {
  if (config == nullptr) {
    return;
  }

  free(config->host_owned);
  free(config->auth_token_owned);
  free(config->redis_host_owned);
  free(config->redis_set_key_owned);
  config->host_owned = nullptr;
  config->auth_token_owned = nullptr;
  config->redis_host_owned = nullptr;
  config->redis_set_key_owned = nullptr;
}

static bool rpc_control_parse_u16(const char *text, uint16_t *out_value) {
  if (text == nullptr || text[0] == '\0' || text[0] == '-') {
    return false;
  }

  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > UINT16_MAX) {
    return false;
  }

  *out_value = (uint16_t)parsed;
  return true;
}

static bool rpc_control_parse_i32_port(const char *text, int32_t *out_port) {
  uint16_t parsed = 0;
  if (!rpc_control_parse_u16(text, &parsed)) {
    return false;
  }

  *out_port = (int32_t)parsed;
  return true;
}

static bool rpc_control_parse_i32_positive(const char *text,
                                           int32_t *out_value) {
  if (text == nullptr || text[0] == '\0' || text[0] == '-' ||
      out_value == nullptr) {
    return false;
  }

  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > (unsigned long)INT32_MAX) {
    return false;
  }

  *out_value = (int32_t)parsed;
  return true;
}

[[nodiscard]] static bool rpc_control_parse_log_level(const char *value,
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

[[nodiscard]] static bool rpc_control_apply_log_level(ulog_level level) {
  auto status = ulog_output_level_set_all(level);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log level '%s' (status=%d)\n",
          ulog_level_to_string(level), (int)status);
  return false;
}

[[nodiscard]] static bool rpc_control_apply_log_color(bool enabled) {
  auto status = ulog_color_config(enabled);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log color '%s' (status=%d)\n",
          enabled ? "true" : "false", (int)status);
  return false;
}

[[nodiscard]] static bool rpc_control_apply_log_style_defaults() {
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

[[nodiscard]] static bool rpc_control_is_blank_string(const char *text) {
  if (text == nullptr) {
    return true;
  }

  for (size_t i = 0; text[i] != '\0'; ++i) {
    if (!isspace((unsigned char)text[i])) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] static bool rpc_control_apply_toml_string(toml_datum_t root,
                                                        const char *key,
                                                        const char **target,
                                                        char **owned) {
  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }
  if (value.type != TOML_STRING) {
    ulog_error("Config key '%s' must be a string\n", key);
    return false;
  }
  if (!rpc_control_replace_string(target, owned, value.u.s)) {
    ulog_error("Out of memory while loading config key '%s'\n", key);
    return false;
  }
  return true;
}

[[nodiscard]] static bool
rpc_control_load_toml_config(rpc_control_config_t *config,
                             const rpc_control_cli_overrides_t *overrides) {
  FILE *fp = fopen(overrides->config_path, "rb");
  if (fp == nullptr) {
    if (!overrides->config_path_set && errno == ENOENT) {
      return true;
    }
    ulog_error("Failed to open config file '%s': %s\n", overrides->config_path,
               strerror(errno));
    return false;
  }

  toml_result_t parsed = toml_parse_file(fp);
  if (fclose(fp) != 0) {
    ulog_error("Failed to close config file '%s'\n", overrides->config_path);
  }
  if (!parsed.ok) {
    ulog_error("Failed to parse config file '%s': %s\n", overrides->config_path,
               parsed.errmsg);
    toml_free(parsed);
    return false;
  }

  bool ok = true;
  ok = ok && rpc_control_apply_toml_string(parsed.toptab, "rpc_control.host",
                                           &config->host, &config->host_owned);
  ok = ok && rpc_control_apply_toml_string(
                 parsed.toptab, "rpc_control.auth_token", &config->auth_token,
                 &config->auth_token_owned);
  ok = ok && rpc_control_apply_toml_string(parsed.toptab, "redis.host",
                                           &config->redis_host,
                                           &config->redis_host_owned);
  ok = ok && rpc_control_apply_toml_string(
                 parsed.toptab, "redis.monitored_set_key",
                 &config->redis_set_key, &config->redis_set_key_owned);

  toml_datum_t rpc_port = toml_seek(parsed.toptab, "rpc_control.port");
  if (ok && rpc_port.type != TOML_UNKNOWN) {
    if (rpc_port.type != TOML_INT64 || rpc_port.u.int64 <= 0 ||
        rpc_port.u.int64 > UINT16_MAX) {
      ulog_error("Config key 'rpc_control.port' must be an integer in "
                 "range 1..65535\n");
      ok = false;
    } else {
      config->port = (int32_t)rpc_port.u.int64;
    }
  }

  toml_datum_t rpc_backlog = toml_seek(parsed.toptab, "rpc_control.backlog");
  if (ok && rpc_backlog.type != TOML_UNKNOWN) {
    if (rpc_backlog.type != TOML_INT64 || rpc_backlog.u.int64 <= 0 ||
        rpc_backlog.u.int64 > INT32_MAX) {
      ulog_error("Config key 'rpc_control.backlog' must be an integer in range "
                 "1..%d\n",
                 INT32_MAX);
      ok = false;
    } else {
      config->backlog = (int32_t)rpc_backlog.u.int64;
    }
  }

  toml_datum_t redis_port = toml_seek(parsed.toptab, "redis.port");
  if (ok && redis_port.type != TOML_UNKNOWN) {
    if (redis_port.type != TOML_INT64 || redis_port.u.int64 <= 0 ||
        redis_port.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'redis.port' must be an integer in range 1..65535\n");
      ok = false;
    } else {
      config->redis_port = (uint16_t)redis_port.u.int64;
    }
  }

  toml_datum_t log_level = toml_seek(parsed.toptab, "logging.level");
  if (ok && log_level.type != TOML_UNKNOWN) {
    if (log_level.type != TOML_STRING) {
      ulog_error("Config key 'logging.level' must be a string\n");
      ok = false;
    } else if (!rpc_control_parse_log_level(log_level.u.s,
                                            &config->log_level)) {
      ulog_error("Config key 'logging.level' has invalid value '%s' (expected "
                 "one of: trace, debug, info, warn, error, fatal)\n",
                 log_level.u.s);
      ok = false;
    }
  }

  toml_datum_t log_color = toml_seek(parsed.toptab, "logging.color");
  if (ok && log_color.type != TOML_UNKNOWN) {
    if (log_color.type != TOML_BOOLEAN) {
      ulog_error("Config key 'logging.color' must be a boolean\n");
      ok = false;
    } else {
      config->log_color = log_color.u.boolean;
    }
  }

  toml_free(parsed);
  return ok;
}

static bool rpc_control_parse_cli(int argc, char *argv[],
                                  rpc_control_cli_overrides_t *overrides) {
  *overrides = (rpc_control_cli_overrides_t){
      .config_path = RPC_CONTROL_DEFAULT_CONFIG_PATH,
  };

  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
      overrides->show_help = true;
      return true;
    }

    if (strcmp(arg, "-c") == 0 || strcmp(arg, "--config") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->config_path = argv[++i];
      overrides->config_path_set = true;
      continue;
    }

    if (strcmp(arg, "-p") == 0 || strcmp(arg, "--port") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->port_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "--listen-host") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->host = argv[++i];
      continue;
    }

    if (strcmp(arg, "--backlog") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->backlog_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "--auth-token") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->auth_token = argv[++i];
      continue;
    }

    if (strcmp(arg, "-H") == 0 || strcmp(arg, "--redis-host") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->redis_host = argv[++i];
      continue;
    }

    if (strcmp(arg, "-r") == 0 || strcmp(arg, "--redis-port") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->redis_port_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "-k") == 0 || strcmp(arg, "--redis-key") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->redis_set_key = argv[++i];
      continue;
    }

    ulog_error("Unrecognized argument: %s\n", arg);
    return false;
  }

  return true;
}

[[nodiscard]] static bool
rpc_control_apply_cli_overrides(rpc_control_config_t *config,
                                const rpc_control_cli_overrides_t *overrides) {
  if (overrides->host != nullptr &&
      !rpc_control_replace_string(&config->host, &config->host_owned,
                                  overrides->host)) {
    ulog_error("Out of memory while applying --listen-host\n");
    return false;
  }

  if (overrides->port_text != nullptr) {
    if (!rpc_control_parse_i32_port(overrides->port_text, &config->port)) {
      ulog_error("Invalid value for --port: %s\n", overrides->port_text);
      return false;
    }
  }

  if (overrides->backlog_text != nullptr) {
    if (!rpc_control_parse_i32_positive(overrides->backlog_text,
                                        &config->backlog)) {
      ulog_error("Invalid value for --backlog: %s\n", overrides->backlog_text);
      return false;
    }
  }

  if (overrides->auth_token != nullptr &&
      !rpc_control_replace_string(&config->auth_token,
                                  &config->auth_token_owned,
                                  overrides->auth_token)) {
    ulog_error("Out of memory while applying --auth-token\n");
    return false;
  }

  if (overrides->redis_host != nullptr &&
      !rpc_control_replace_string(&config->redis_host,
                                  &config->redis_host_owned,
                                  overrides->redis_host)) {
    ulog_error("Out of memory while applying --redis-host\n");
    return false;
  }

  if (overrides->redis_port_text != nullptr) {
    if (!rpc_control_parse_u16(overrides->redis_port_text,
                               &config->redis_port)) {
      ulog_error("Invalid value for --redis-port: %s\n",
                 overrides->redis_port_text);
      return false;
    }
  }

  if (overrides->redis_set_key != nullptr &&
      !rpc_control_replace_string(&config->redis_set_key,
                                  &config->redis_set_key_owned,
                                  overrides->redis_set_key)) {
    ulog_error("Out of memory while applying --redis-key\n");
    return false;
  }

  return true;
}

[[nodiscard]] static bool
rpc_control_validate_security_config(const rpc_control_config_t *config) {
  if (config == nullptr) {
    return false;
  }

  if (rpc_control_is_blank_string(config->auth_token)) {
    ulog_error(
        "rpc_control.auth_token is required. Configure it in config.toml "
        "or pass --auth-token.\n");
    return false;
  }

  if (strcmp(config->auth_token, RPC_CONTROL_DEFAULT_AUTH_TOKEN) == 0) {
    ulog_error("rpc_control.auth_token must not use the default placeholder "
               "value '%s'.\n",
               RPC_CONTROL_DEFAULT_AUTH_TOKEN);
    return false;
  }

  return true;
}

[[nodiscard]] static bool
rpc_control_is_connection_authenticated(jsonrpc_conn_t *conn) {
  if (conn == nullptr) {
    return false;
  }

  for (size_t i = 0; i < g_runtime.authenticated_connection_count; ++i) {
    if (g_runtime.authenticated_connections[i] == conn) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] static bool
rpc_control_mark_connection_authenticated(jsonrpc_conn_t *conn) {
  if (conn == nullptr) {
    return false;
  }

  if (rpc_control_is_connection_authenticated(conn)) {
    return true;
  }

  if (g_runtime.authenticated_connection_count ==
      g_runtime.authenticated_connection_capacity) {
    size_t new_capacity = g_runtime.authenticated_connection_capacity == 0
                              ? 16
                              : g_runtime.authenticated_connection_capacity * 2;
    if (new_capacity < g_runtime.authenticated_connection_count ||
        new_capacity >
            SIZE_MAX / sizeof(*g_runtime.authenticated_connections)) {
      return false;
    }

    auto resized = (jsonrpc_conn_t **)realloc(
        g_runtime.authenticated_connections,
        new_capacity * sizeof(*g_runtime.authenticated_connections));
    if (resized == nullptr) {
      return false;
    }
    g_runtime.authenticated_connections = resized;
    g_runtime.authenticated_connection_capacity = new_capacity;
  }

  g_runtime
      .authenticated_connections[g_runtime.authenticated_connection_count++] =
      conn;
  return true;
}

static void rpc_control_forget_connection_auth(jsonrpc_conn_t *conn) {
  if (conn == nullptr || g_runtime.authenticated_connection_count == 0) {
    return;
  }

  for (size_t i = 0; i < g_runtime.authenticated_connection_count; ++i) {
    if (g_runtime.authenticated_connections[i] != conn) {
      continue;
    }
    g_runtime.authenticated_connections[i] =
        g_runtime.authenticated_connections
            [g_runtime.authenticated_connection_count - 1];
    g_runtime.authenticated_connection_count -= 1;
    break;
  }

  if (g_runtime.authenticated_connection_count == 0) {
    free(g_runtime.authenticated_connections);
    g_runtime.authenticated_connections = nullptr;
    g_runtime.authenticated_connection_capacity = 0;
  }
}

static bool rpc_control_connect_redis(const rpc_control_config_t *config) {
  g_runtime.auth_token = config->auth_token;
  g_runtime.redis = redisConnect(config->redis_host, (int)config->redis_port);
  if (g_runtime.redis == nullptr) {
    ulog_error("Failed to allocate Redis context\n");
    return false;
  }

  if (g_runtime.redis->err != 0) {
    ulog_error("Failed to connect to Redis at %s:%u: %s\n", config->redis_host,
               config->redis_port, g_runtime.redis->errstr);
    redisFree(g_runtime.redis);
    g_runtime.redis = nullptr;
    return false;
  }

  g_runtime.redis_set_key = config->redis_set_key;
  ulog_info("Connected to Redis at %s:%u (set=%s)", config->redis_host,
            config->redis_port, g_runtime.redis_set_key);
  return true;
}

static void rpc_control_disconnect_redis() {
  if (g_runtime.redis != nullptr) {
    redisFree(g_runtime.redis);
    g_runtime.redis = nullptr;
  }
  free(g_runtime.authenticated_connections);
  g_runtime.authenticated_connections = nullptr;
  g_runtime.authenticated_connection_count = 0;
  g_runtime.authenticated_connection_capacity = 0;
  g_runtime.auth_token = nullptr;
}

static redisReply *rpc_control_redis_command(const char *format, ...) {
  if (g_runtime.redis == nullptr || format == nullptr) {
    return nullptr;
  }

  va_list ap;
  va_start(ap, format);
  redisReply *reply = redisvCommand(g_runtime.redis, format, ap);
  va_end(ap);

  if (reply != nullptr) {
    return reply;
  }

  ulog_error("Redis command failed, reconnecting: %s\n",
             g_runtime.redis->errstr);
  if (redisReconnect(g_runtime.redis) != REDIS_OK) {
    ulog_error("Redis reconnect failed: %s\n", g_runtime.redis->errstr);
    return nullptr;
  }

  va_start(ap, format);
  reply = redisvCommand(g_runtime.redis, format, ap);
  va_end(ap);

  if (reply == nullptr) {
    ulog_error("Redis command failed after reconnect: %s\n",
               g_runtime.redis->errstr);
  }

  return reply;
}

[[nodiscard]] static bool
rpc_control_normalize_address(const char *address, char *normalized,
                              size_t normalized_capacity) {
  if (address == nullptr || normalized == nullptr || normalized_capacity == 0) {
    return false;
  }

  size_t length = strlen(address);
  if (length != 42 || length + 1 > normalized_capacity) {
    return false;
  }

  if (address[0] != '0' || (address[1] != 'x' && address[1] != 'X')) {
    return false;
  }

  normalized[0] = '0';
  normalized[1] = 'x';
  for (size_t i = 2; i < length; ++i) {
    unsigned char ch = (unsigned char)address[i];
    if (!isxdigit(ch)) {
      return false;
    }
    normalized[i] = (char)tolower(ch);
  }
  normalized[length] = '\0';
  return true;
}

static bool rpc_control_json_array_append_string(JSON_Array *array,
                                                 const char *value) {
  if (array == nullptr || value == nullptr) {
    return false;
  }

  return json_array_append_string(array, value) == JSONSuccess;
}

static bool rpc_control_json_object_add_array(JSON_Object *object,
                                              const char *name,
                                              JSON_Array **out_array) {
  if (object == nullptr || name == nullptr || out_array == nullptr) {
    return false;
  }

  JSON_Value *array_value = json_value_init_array();
  if (array_value == nullptr) {
    return false;
  }

  if (json_object_set_value(object, name, array_value) != JSONSuccess) {
    json_value_free(array_value);
    return false;
  }

  *out_array = json_value_get_array(array_value);
  return *out_array != nullptr;
}

static bool
rpc_control_parse_address_input(const JSON_Value *params,
                                rpc_control_address_input_t *out_input,
                                const char **out_error_message) {
  if (out_input == nullptr || out_error_message == nullptr) {
    return false;
  }

  *out_input = (rpc_control_address_input_t){0};
  *out_error_message = nullptr;

  if (params == nullptr) {
    *out_error_message = "Missing params";
    return false;
  }

  JSON_Value_Type type = json_value_get_type(params);
  if (type == JSONString) {
    out_input->single = json_value_get_string(params);
    return true;
  }

  if (type == JSONArray) {
    out_input->array = json_value_get_array(params);
    return true;
  }

  if (type != JSONObject) {
    *out_error_message = "Expected string, array, or object params";
    return false;
  }

  const JSON_Object *params_obj = json_value_get_object(params);

  JSON_Value *addresses_value = json_object_get_value(params_obj, "addresses");
  if (addresses_value != nullptr) {
    if (json_value_get_type(addresses_value) != JSONArray) {
      *out_error_message = "Field 'addresses' must be an array";
      return false;
    }
    out_input->array = json_value_get_array(addresses_value);
    return true;
  }

  JSON_Value *address_value = json_object_get_value(params_obj, "address");
  if (address_value != nullptr) {
    if (json_value_get_type(address_value) != JSONString) {
      *out_error_message = "Field 'address' must be a string";
      return false;
    }
    out_input->single = json_value_get_string(address_value);
    return true;
  }

  *out_error_message = "Expected 'address' or 'addresses'";
  return false;
}

static size_t
rpc_control_address_input_count(const rpc_control_address_input_t *input) {
  if (input == nullptr) {
    return 0;
  }

  if (input->array != nullptr) {
    return json_array_get_count(input->array);
  }

  if (input->single != nullptr) {
    return 1;
  }

  return 0;
}

static const char *
rpc_control_address_input_get(const rpc_control_address_input_t *input,
                              size_t index, bool *out_is_string) {
  if (out_is_string == nullptr || input == nullptr) {
    return nullptr;
  }

  *out_is_string = false;

  if (input->array != nullptr) {
    const JSON_Value *item = json_array_get_value(input->array, index);
    if (item == nullptr || json_value_get_type(item) != JSONString) {
      return RPC_CONTROL_NON_STRING;
    }

    *out_is_string = true;
    return json_value_get_string(item);
  }

  if (input->single != nullptr && index == 0) {
    *out_is_string = true;
    return input->single;
  }

  return nullptr;
}

static bool rpc_control_redis_sadd(const char *member, bool *out_added) {
  if (member == nullptr || out_added == nullptr) {
    return false;
  }

  redisReply *reply =
      rpc_control_redis_command("SADD %s %s", g_runtime.redis_set_key, member);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER || reply->type == REDIS_REPLY_BOOL) {
    *out_added = reply->integer != 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis SADD error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for SADD: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_srem(const char *member, bool *out_removed) {
  if (member == nullptr || out_removed == nullptr) {
    return false;
  }

  redisReply *reply =
      rpc_control_redis_command("SREM %s %s", g_runtime.redis_set_key, member);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER || reply->type == REDIS_REPLY_BOOL) {
    *out_removed = reply->integer != 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis SREM error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for SREM: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_sismember(const char *member, bool *out_present) {
  if (member == nullptr || out_present == nullptr) {
    return false;
  }

  redisReply *reply = rpc_control_redis_command(
      "SISMEMBER %s %s", g_runtime.redis_set_key, member);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER || reply->type == REDIS_REPLY_BOOL) {
    *out_present = reply->integer != 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis SISMEMBER error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for SISMEMBER: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_scard(size_t *out_count) {
  if (out_count == nullptr) {
    return false;
  }

  redisReply *reply =
      rpc_control_redis_command("SCARD %s", g_runtime.redis_set_key);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER) {
    if (reply->integer < 0) {
      ok = false;
    } else {
      *out_count = (size_t)reply->integer;
    }
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis SCARD error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for SCARD: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_del_key(bool *out_deleted) {
  if (out_deleted == nullptr) {
    return false;
  }

  redisReply *reply =
      rpc_control_redis_command("DEL %s", g_runtime.redis_set_key);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER || reply->type == REDIS_REPLY_BOOL) {
    *out_deleted = reply->integer != 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis DEL error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for DEL: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_ping(bool *out_healthy) {
  if (out_healthy == nullptr) {
    return false;
  }

  redisReply *reply = rpc_control_redis_command("PING");
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_STRING) {
    *out_healthy = reply->str != nullptr && strcasecmp(reply->str, "PONG") == 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis PING error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for PING: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_redis_smembers(JSON_Array *out_array) {
  if (out_array == nullptr) {
    return false;
  }

  redisReply *reply =
      rpc_control_redis_command("SMEMBERS %s", g_runtime.redis_set_key);
  if (reply == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_ARRAY || reply->type == REDIS_REPLY_SET) {
    for (size_t i = 0; i < reply->elements; ++i) {
      redisReply *item = reply->element[i];
      if (item == nullptr || item->str == nullptr) {
        continue;
      }
      if (!rpc_control_json_array_append_string(out_array, item->str)) {
        ok = false;
        break;
      }
    }
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis SMEMBERS error: %s\n",
               reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error("Unexpected Redis reply type for SMEMBERS: %d\n", reply->type);
    ok = false;
  }

  freeReplyObject(reply);
  return ok;
}

static bool rpc_control_set_error(jsonrpc_response_t *response, int32_t code,
                                  const char *message) {
  if (response == nullptr) {
    return false;
  }

  response->error_code = code;
  response->error_message = message;
  return true;
}

static bool rpc_control_handle_add(const JSON_Value *params,
                                   jsonrpc_response_t *response) {
  rpc_control_address_input_t input = {0};
  const char *parse_error = nullptr;
  if (!rpc_control_parse_address_input(params, &input, &parse_error)) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 parse_error);
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  JSON_Array *added = nullptr;
  JSON_Array *already_present = nullptr;
  JSON_Array *invalid = nullptr;
  if (result_obj == nullptr ||
      !rpc_control_json_object_add_array(result_obj, "added", &added) ||
      !rpc_control_json_object_add_array(result_obj, "already_present",
                                         &already_present) ||
      !rpc_control_json_object_add_array(result_obj, "invalid", &invalid)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  size_t requested_count = 0;
  size_t added_count = 0;
  size_t existing_count = 0;
  size_t invalid_count = 0;
  size_t count = rpc_control_address_input_count(&input);

  for (size_t i = 0; i < count; ++i) {
    bool is_string = false;
    const char *address = rpc_control_address_input_get(&input, i, &is_string);
    ++requested_count;

    if (!is_string || address == nullptr) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid,
                                                RPC_CONTROL_NON_STRING)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    char normalized[RPC_CONTROL_ADDRESS_CAPACITY] = {0};
    if (!rpc_control_normalize_address(address, normalized,
                                       sizeof(normalized))) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid, address)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    bool added_to_set = false;
    if (!rpc_control_redis_sadd(normalized, &added_to_set)) {
      json_value_free(result);
      return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                   "Redis SADD failed");
    }

    if (added_to_set) {
      ++added_count;
      if (!rpc_control_json_array_append_string(added, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    } else {
      ++existing_count;
      if (!rpc_control_json_array_append_string(already_present, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    }
  }

  bool ok = true;
  ok = ok && json_object_set_number(result_obj, "requested_count",
                                    (double)requested_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "added_count",
                                    (double)added_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "already_present_count",
                                    (double)existing_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "invalid_count",
                                    (double)invalid_count) == JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_remove(const JSON_Value *params,
                                      jsonrpc_response_t *response) {
  rpc_control_address_input_t input = {0};
  const char *parse_error = nullptr;
  if (!rpc_control_parse_address_input(params, &input, &parse_error)) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 parse_error);
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  JSON_Array *removed = nullptr;
  JSON_Array *missing = nullptr;
  JSON_Array *invalid = nullptr;
  if (result_obj == nullptr ||
      !rpc_control_json_object_add_array(result_obj, "removed", &removed) ||
      !rpc_control_json_object_add_array(result_obj, "missing", &missing) ||
      !rpc_control_json_object_add_array(result_obj, "invalid", &invalid)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  size_t requested_count = 0;
  size_t removed_count = 0;
  size_t missing_count = 0;
  size_t invalid_count = 0;
  size_t count = rpc_control_address_input_count(&input);

  for (size_t i = 0; i < count; ++i) {
    bool is_string = false;
    const char *address = rpc_control_address_input_get(&input, i, &is_string);
    ++requested_count;

    if (!is_string || address == nullptr) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid,
                                                RPC_CONTROL_NON_STRING)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    char normalized[RPC_CONTROL_ADDRESS_CAPACITY] = {0};
    if (!rpc_control_normalize_address(address, normalized,
                                       sizeof(normalized))) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid, address)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    bool removed_from_set = false;
    if (!rpc_control_redis_srem(normalized, &removed_from_set)) {
      json_value_free(result);
      return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                   "Redis SREM failed");
    }

    if (removed_from_set) {
      ++removed_count;
      if (!rpc_control_json_array_append_string(removed, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    } else {
      ++missing_count;
      if (!rpc_control_json_array_append_string(missing, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    }
  }

  bool ok = true;
  ok = ok && json_object_set_number(result_obj, "requested_count",
                                    (double)requested_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "removed_count",
                                    (double)removed_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "missing_count",
                                    (double)missing_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "invalid_count",
                                    (double)invalid_count) == JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_has(const JSON_Value *params,
                                   jsonrpc_response_t *response) {
  rpc_control_address_input_t input = {0};
  const char *parse_error = nullptr;
  if (!rpc_control_parse_address_input(params, &input, &parse_error)) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 parse_error);
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  JSON_Array *present = nullptr;
  JSON_Array *absent = nullptr;
  JSON_Array *invalid = nullptr;
  if (result_obj == nullptr ||
      !rpc_control_json_object_add_array(result_obj, "present", &present) ||
      !rpc_control_json_object_add_array(result_obj, "absent", &absent) ||
      !rpc_control_json_object_add_array(result_obj, "invalid", &invalid)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  size_t requested_count = 0;
  size_t present_count = 0;
  size_t absent_count = 0;
  size_t invalid_count = 0;
  size_t count = rpc_control_address_input_count(&input);

  for (size_t i = 0; i < count; ++i) {
    bool is_string = false;
    const char *address = rpc_control_address_input_get(&input, i, &is_string);
    ++requested_count;

    if (!is_string || address == nullptr) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid,
                                                RPC_CONTROL_NON_STRING)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    char normalized[RPC_CONTROL_ADDRESS_CAPACITY] = {0};
    if (!rpc_control_normalize_address(address, normalized,
                                       sizeof(normalized))) {
      ++invalid_count;
      if (!rpc_control_json_array_append_string(invalid, address)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
      continue;
    }

    bool is_present = false;
    if (!rpc_control_redis_sismember(normalized, &is_present)) {
      json_value_free(result);
      return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                   "Redis SISMEMBER failed");
    }

    if (is_present) {
      ++present_count;
      if (!rpc_control_json_array_append_string(present, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    } else {
      ++absent_count;
      if (!rpc_control_json_array_append_string(absent, normalized)) {
        json_value_free(result);
        return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                     "Out of memory");
      }
    }
  }

  bool ok = true;
  ok = ok && json_object_set_number(result_obj, "requested_count",
                                    (double)requested_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "present_count",
                                    (double)present_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "absent_count",
                                    (double)absent_count) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "invalid_count",
                                    (double)invalid_count) == JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_count(jsonrpc_response_t *response) {
  size_t count = 0;
  if (!rpc_control_redis_scard(&count)) {
    return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                 "Redis SCARD failed");
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  bool ok = result_obj != nullptr;
  ok = ok && json_object_set_string(result_obj, "set_key",
                                    g_runtime.redis_set_key) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "count", (double)count) ==
                 JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_list(jsonrpc_response_t *response) {
  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  JSON_Array *addresses = nullptr;
  if (result_obj == nullptr ||
      !rpc_control_json_object_add_array(result_obj, "addresses", &addresses)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  if (!rpc_control_redis_smembers(addresses)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                 "Redis SMEMBERS failed");
  }

  size_t count = json_array_get_count(addresses);
  bool ok = true;
  ok = ok && json_object_set_string(result_obj, "set_key",
                                    g_runtime.redis_set_key) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "count", (double)count) ==
                 JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_clear(const JSON_Value *params,
                                     jsonrpc_response_t *response) {
  if (params == nullptr || json_value_get_type(params) != JSONObject) {
    return rpc_control_set_error(
        response, JSONRPC_ERR_INVALID_PARAMS,
        "Expected object params with {\"confirm\": true}");
  }

  const JSON_Object *params_obj = json_value_get_object(params);
  JSON_Value *confirm_value = json_object_get_value(params_obj, "confirm");
  if (confirm_value == nullptr ||
      json_value_get_type(confirm_value) != JSONBoolean ||
      json_value_get_boolean(confirm_value) != JSONBooleanTrue) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 "monitor_clear requires {\"confirm\": true}");
  }

  size_t before_count = 0;
  if (!rpc_control_redis_scard(&before_count)) {
    return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                 "Redis SCARD failed");
  }

  bool deleted = false;
  if (!rpc_control_redis_del_key(&deleted)) {
    return rpc_control_set_error(response, JSONRPC_ERR_SERVER,
                                 "Redis DEL failed");
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  bool ok = result_obj != nullptr;
  ok = ok && json_object_set_string(result_obj, "set_key",
                                    g_runtime.redis_set_key) == JSONSuccess;
  ok = ok && json_object_set_boolean(result_obj, "deleted",
                                     deleted ? JSONBooleanTrue
                                             : JSONBooleanFalse) == JSONSuccess;
  ok = ok && json_object_set_number(result_obj, "removed_count",
                                    deleted ? (double)before_count : 0.0) ==
                 JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_auth(jsonrpc_conn_t *conn,
                                    const JSON_Value *params,
                                    jsonrpc_response_t *response) {
  if (response == nullptr) {
    return false;
  }

  if (conn == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Missing connection context");
  }

  if (params == nullptr || json_value_get_type(params) != JSONObject) {
    return rpc_control_set_error(
        response, JSONRPC_ERR_INVALID_PARAMS,
        "Expected object params with {\"token\": \"...\"}");
  }

  const JSON_Object *params_obj = json_value_get_object(params);
  if (params_obj == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 "Invalid auth params");
  }

  const char *token = json_object_get_string(params_obj, "token");
  if (token == nullptr) {
    token = json_object_get_string(params_obj, "auth_token");
  }
  if (rpc_control_is_blank_string(token)) {
    return rpc_control_set_error(response, JSONRPC_ERR_INVALID_PARAMS,
                                 "Field 'token' must be a non-empty string");
  }

  if (g_runtime.auth_token == nullptr ||
      strcmp(token, g_runtime.auth_token) != 0) {
    return rpc_control_set_error(response, JSONRPC_ERR_UNAUTHORIZED,
                                 "Invalid authentication token");
  }

  if (!rpc_control_mark_connection_authenticated(conn)) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  bool ok = result_obj != nullptr;
  ok = ok && json_object_set_boolean(result_obj, "authenticated",
                                     JSONBooleanTrue) == JSONSuccess;
  ok = ok && json_object_set_string(result_obj, "status", "ok") == JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_health(jsonrpc_response_t *response) {
  bool redis_healthy = false;
  bool ping_ok = rpc_control_redis_ping(&redis_healthy);

  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  bool healthy = ping_ok && redis_healthy;
  bool ok = result_obj != nullptr;
  ok = ok && json_object_set_string(result_obj, "status",
                                    healthy ? "ok" : "degraded") == JSONSuccess;
  ok = ok && json_object_set_boolean(result_obj, "redis_command_ok",
                                     ping_ok ? JSONBooleanTrue
                                             : JSONBooleanFalse) == JSONSuccess;
  ok = ok &&
       json_object_set_boolean(result_obj, "redis_healthy",
                               redis_healthy ? JSONBooleanTrue
                                             : JSONBooleanFalse) == JSONSuccess;
  ok = ok && json_object_set_string(result_obj, "set_key",
                                    g_runtime.redis_set_key) == JSONSuccess;

  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

static bool rpc_control_handle_methods(jsonrpc_response_t *response) {
  JSON_Value *result = json_value_init_object();
  if (result == nullptr) {
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  JSON_Object *result_obj = json_value_get_object(result);
  JSON_Array *methods = nullptr;
  if (result_obj == nullptr ||
      !rpc_control_json_object_add_array(result_obj, "methods", &methods)) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  for (size_t i = 0; i < sizeof(RPC_CONTROL_METHOD_NAMES) /
                             sizeof(RPC_CONTROL_METHOD_NAMES[0]);
       ++i) {
    if (!rpc_control_json_array_append_string(methods,
                                              RPC_CONTROL_METHOD_NAMES[i])) {
      json_value_free(result);
      return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                   "Out of memory");
    }
  }

  bool ok = json_object_set_number(
                result_obj, "count",
                (double)sizeof(RPC_CONTROL_METHOD_NAMES) /
                    (double)sizeof(RPC_CONTROL_METHOD_NAMES[0])) == JSONSuccess;
  if (!ok) {
    json_value_free(result);
    return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                 "Out of memory");
  }

  response->result = result;
  return true;
}

void rpc_control_on_open([[maybe_unused]] jsonrpc_conn_t *conn) {
  ulog_info("[rpc-control] JSON-RPC connection opened");
}

bool rpc_control_on_request([[maybe_unused]] jsonrpc_conn_t *conn,
                            const char *method, const JSON_Value *params,
                            jsonrpc_response_t *response) {
  if (method == nullptr || response == nullptr) {
    return false;
  }

  if (strcmp(method, "ping") == 0) {
    response->result = json_value_init_string("pong");
    if (response->result == nullptr) {
      return rpc_control_set_error(response, JSONRPC_ERR_INTERNAL,
                                   "Out of memory");
    }
    return true;
  }

  if (strcmp(method, "auth") == 0) {
    return rpc_control_handle_auth(conn, params, response);
  }

  if (!rpc_control_is_connection_authenticated(conn)) {
    return rpc_control_set_error(
        response, JSONRPC_ERR_UNAUTHORIZED,
        "Unauthorized: call 'auth' with {\"token\":\"...\"} first");
  }

  if (strcmp(method, "health") == 0) {
    return rpc_control_handle_health(response);
  }

  if (strcmp(method, "methods") == 0) {
    return rpc_control_handle_methods(response);
  }

  if (strcmp(method, "monitor_add") == 0 ||
      strcmp(method, "add_address") == 0 ||
      strcmp(method, "add_addresses") == 0) {
    return rpc_control_handle_add(params, response);
  }

  if (strcmp(method, "monitor_remove") == 0 ||
      strcmp(method, "remove_address") == 0 ||
      strcmp(method, "remove_addresses") == 0) {
    return rpc_control_handle_remove(params, response);
  }

  if (strcmp(method, "monitor_has") == 0 ||
      strcmp(method, "is_monitored") == 0) {
    return rpc_control_handle_has(params, response);
  }

  if (strcmp(method, "monitor_count") == 0) {
    return rpc_control_handle_count(response);
  }

  if (strcmp(method, "monitor_list") == 0) {
    return rpc_control_handle_list(response);
  }

  if (strcmp(method, "monitor_clear") == 0) {
    return rpc_control_handle_clear(params, response);
  }

  return false;
}

void rpc_control_on_notification([[maybe_unused]] jsonrpc_conn_t *conn,
                                 const char *method,
                                 [[maybe_unused]] const JSON_Value *params) {
  if (method != nullptr) {
    ulog_info("[rpc-control] Notification: %s", method);
  }
}

void rpc_control_on_close([[maybe_unused]] jsonrpc_conn_t *conn) {
  rpc_control_forget_connection_auth(conn);
  ulog_info("[rpc-control] JSON-RPC connection closed");
}

static void rpc_control_on_signal(uv_signal_t *handle, int signum) {
  g_shutdown_signal = signum;
  (void)uv_signal_stop(handle);
  uv_close((uv_handle_t *)handle, nullptr);
  server_request_shutdown();
}

int main(int argc, char **argv) {
  if (!rpc_control_apply_log_style_defaults()) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  rpc_control_cli_overrides_t overrides = {0};
  if (!rpc_control_parse_cli(argc, argv, &overrides)) {
    rpc_control_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    rpc_control_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  rpc_control_config_t config = {0};
  rpc_control_config_set_defaults(&config);

  bool config_ok = rpc_control_load_toml_config(&config, &overrides);
  config_ok = config_ok && rpc_control_apply_cli_overrides(&config, &overrides);
  if (!config_ok) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }
  if (!rpc_control_apply_log_color(config.log_color)) {
    rpc_control_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  if (!rpc_control_apply_log_level(config.log_level)) {
    rpc_control_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  if (!rpc_control_validate_security_config(&config)) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  if (!rpc_control_connect_redis(&config)) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  ulog_info("Starting RPC control server on %s:%" PRId32 " (backlog=%" PRId32
            ")",
            config.host, config.port, config.backlog);
  ulog_info("RPC control authentication enabled (token required via 'auth' "
            "method)");

  jsonrpc_callbacks_t callbacks = {
      .on_open = rpc_control_on_open,
      .on_close = rpc_control_on_close,
      .on_request = rpc_control_on_request,
      .on_notification = rpc_control_on_notification,
  };

  uv_loop_t *loop = uv_default_loop();
  uv_signal_t sigint_handle = {0};
  uv_signal_t sigterm_handle = {0};

  if (loop != nullptr && uv_signal_init(loop, &sigint_handle) == 0) {
    (void)uv_signal_start(&sigint_handle, rpc_control_on_signal, SIGINT);
  }
  if (loop != nullptr && uv_signal_init(loop, &sigterm_handle) == 0) {
    (void)uv_signal_start(&sigterm_handle, rpc_control_on_signal, SIGTERM);
  }

  start_jsonrpc_server(config.host, config.port, config.backlog, callbacks);
  rpc_control_disconnect_redis();
  rpc_control_config_cleanup(&config);

  if (g_shutdown_signal != 0) {
    ulog_info("Shutdown signal received (%d)", (int)g_shutdown_signal);
  }

  return EXIT_SUCCESS;
}
