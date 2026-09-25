#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>
#include <stdckdint.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "rpc_control/config_internal.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

#if defined(USE_MIMALLOC)
#include "hiredis/alloc.h"
#include <mimalloc.h>
#include <uv.h>
#endif

/*
 * Configuration boundary for rpc_control.
 *
 * CLI and TOML strings are untrusted borrowed views. Accepted values are
 * copied into explicit owned fields and released by rpc_control_config_cleanup.
 * The authentication token is never included in diagnostic output.
 */
constexpr int32_t RPC_CONTROL_DEFAULT_PORT = 8080;
constexpr char RPC_CONTROL_DEFAULT_HOST[] = "127.0.0.1";
constexpr int32_t RPC_CONTROL_DEFAULT_BACKLOG = 4096;
constexpr char RPC_CONTROL_DEFAULT_CONFIG_PATH[] = "conf/config.toml";
constexpr char RPC_CONTROL_DEFAULT_AUTH_TOKEN[] = "CHANGE_ME";
constexpr ulog_level RPC_CONTROL_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
constexpr bool RPC_CONTROL_DEFAULT_LOG_COLOR = true;
constexpr char RPC_CONTROL_DEFAULT_REDIS_HOST[] = "127.0.0.1";
constexpr uint16_t RPC_CONTROL_DEFAULT_REDIS_PORT = 6379;
constexpr char RPC_CONTROL_DEFAULT_REDIS_SET_KEY[] = "monitored_addresses";
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

[[nodiscard]] bool rpc_control_configure_allocator_overrides() {
#if defined(USE_MIMALLOC)
  /* TOML allocates through realloc(nullptr, size), so both callbacks must
   * belong to the same allocator family. */
  toml_option_t toml_options = toml_default_option();
  toml_options.mem_realloc = mi_realloc;
  toml_options.mem_free = mi_free;
  toml_set_option(toml_options);

  hiredisAllocFuncs hiredis_allocators = {
      .mallocFn = mi_malloc,
      .callocFn = mi_calloc,
      .reallocFn = mi_realloc,
      .strdupFn = mi_strdup,
      .freeFn = mi_free,
  };
  (void)hiredisSetAllocators(&hiredis_allocators);
  if (uv_replace_allocator(mi_malloc, mi_realloc, mi_calloc, mi_free) != 0) {
    return false;
  }
#endif
  return true;
}

void rpc_control_print_usage(const char *program_name) {
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

void rpc_control_config_set_defaults(rpc_control_config_t *config) {
  if (config == nullptr) {
    return;
  }

  config->host = RPC_CONTROL_DEFAULT_HOST;
  config->port = (app_port_t){.value = RPC_CONTROL_DEFAULT_PORT};
  config->backlog =
      (app_socket_backlog_t){.value = RPC_CONTROL_DEFAULT_BACKLOG};
  config->auth_token = RPC_CONTROL_DEFAULT_AUTH_TOKEN;
  config->log_level = RPC_CONTROL_DEFAULT_LOG_LEVEL;
  config->log_color = RPC_CONTROL_DEFAULT_LOG_COLOR;
  config->redis_host = RPC_CONTROL_DEFAULT_REDIS_HOST;
  config->redis_port = (app_port_t){.value = RPC_CONTROL_DEFAULT_REDIS_PORT};
  config->redis_set_key = RPC_CONTROL_DEFAULT_REDIS_SET_KEY;
}

void rpc_control_config_cleanup(rpc_control_config_t *config) {
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

[[nodiscard]] bool rpc_control_apply_log_level(ulog_level level) {
  auto status = ulog_output_level_set_all(level);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log level '%s' (status=%d)\n",
          ulog_level_to_string(level), (int)status);
  return false;
}

[[nodiscard]] bool rpc_control_apply_log_color(bool enabled) {
  auto status = ulog_color_config(enabled);
  if (status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED) {
    return true;
  }

  fprintf(stderr, "Failed to set log color '%s' (status=%d)\n",
          enabled ? "true" : "false", (int)status);
  return false;
}

[[nodiscard]] bool rpc_control_apply_log_style_defaults() {
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

/* Load TOML values over defaults while retaining ownership in config. */
[[nodiscard]] static bool rpc_control_load_toml_config_impl(
    rpc_control_config_t *config,
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
      config->port.value = (uint16_t)rpc_port.u.int64;
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
      config->backlog.value = (int32_t)rpc_backlog.u.int64;
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
      config->redis_port.value = (uint16_t)redis_port.u.int64;
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

[[nodiscard]] app_config_status_t
rpc_control_load_toml_config(rpc_control_config_t *config,
                             const rpc_control_cli_overrides_t *overrides) {
  return rpc_control_load_toml_config_impl(config, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_LOAD_ERROR;
}

/* Parse argv into borrowed views; this phase does not mutate config. */
[[nodiscard]] static bool
rpc_control_parse_cli_impl(int argc, char *argv[],
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

[[nodiscard]] app_config_status_t
rpc_control_parse_cli(int argc, char *argv[],
                      rpc_control_cli_overrides_t *overrides) {
  return rpc_control_parse_cli_impl(argc, argv, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_INVALID_ARGUMENT;
}

/* Validate and apply CLI values as the highest-precedence layer. */
[[nodiscard]] static bool rpc_control_apply_cli_overrides_impl(
    rpc_control_config_t *config,
    const rpc_control_cli_overrides_t *overrides) {
  if (overrides->host != nullptr &&
      !rpc_control_replace_string(&config->host, &config->host_owned,
                                  overrides->host)) {
    ulog_error("Out of memory while applying --listen-host\n");
    return false;
  }

  if (overrides->port_text != nullptr) {
    if (!rpc_control_parse_u16(overrides->port_text, &config->port.value)) {
      ulog_error("Invalid value for --port: %s\n", overrides->port_text);
      return false;
    }
  }

  if (overrides->backlog_text != nullptr) {
    if (!rpc_control_parse_i32_positive(overrides->backlog_text,
                                        &config->backlog.value)) {
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
                               &config->redis_port.value)) {
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

[[nodiscard]] app_config_status_t
rpc_control_apply_cli_overrides(rpc_control_config_t *config,
                                const rpc_control_cli_overrides_t *overrides) {
  return rpc_control_apply_cli_overrides_impl(config, overrides)
             ? APP_CONFIG_STATUS_OK
             : APP_CONFIG_STATUS_INVALID_VALUE;
}

[[nodiscard]] bool
rpc_control_validate_security_config(const rpc_control_config_t *config) {
  if (config == nullptr) {
    return false;
  }

  if (rpc_control_is_blank_string(config->auth_token)) {
    ulog_error(
        "rpc_control.auth_token is required. Configure it in conf/config.toml "
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
