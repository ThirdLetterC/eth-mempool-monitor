#define _POSIX_C_SOURCE 200809L
#include "websocket-client/rabbitmq_tx_console_internal.h"
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
 * Console configuration boundary.
 *
 * Parsed strings are copied into owned fields, numeric values are range
 * checked, and credentials are cleared before their storage is released.
 */
constexpr char APP_DEFAULT_CONFIG_PATH[] = "conf/config.toml";
constexpr char APP_DEFAULT_RABBITMQ_HOST[] = "127.0.0.1";
constexpr uint16_t APP_DEFAULT_RABBITMQ_PORT = 5'672;
constexpr char APP_DEFAULT_RABBITMQ_USERNAME[] = "guest";
constexpr char APP_DEFAULT_RABBITMQ_PASSWORD[] = "guest";
constexpr char APP_DEFAULT_RABBITMQ_VHOST[] = "/";
constexpr char APP_DEFAULT_RABBITMQ_QUEUE[] = "monitored_transactions";
constexpr bool APP_DEFAULT_RABBITMQ_QUEUE_DURABLE = true;
constexpr uint16_t APP_DEFAULT_RABBITMQ_CHANNEL = 1;
constexpr uint16_t APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS = 30;
constexpr uint32_t APP_DEFAULT_READ_TIMEOUT_SECONDS = 1;
constexpr uint16_t APP_DEFAULT_PREFETCH_COUNT = 200;
constexpr bool APP_DEFAULT_AUTO_ACK = false;
constexpr ulog_level APP_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
constexpr bool APP_DEFAULT_LOG_COLOR = true;

void app_print_usage(const char *program_name) {
  printf("Usage: %s [options]\n", program_name);
  printf("\n");
  printf("Options:\n");
  printf("  -c, --config <file>               TOML config path (default: %s)\n",
         APP_DEFAULT_CONFIG_PATH);
  printf("      --rabbitmq-host <host>        RabbitMQ host\n");
  printf("      --rabbitmq-port <port>        RabbitMQ port (1..65535)\n");
  printf("      --rabbitmq-user <user>        RabbitMQ username\n");
  printf("      --rabbitmq-password <pass>    RabbitMQ password\n");
  printf("      --rabbitmq-vhost <vhost>      RabbitMQ virtual host\n");
  printf("      --rabbitmq-queue <name>       RabbitMQ queue name\n");
  printf("      --rabbitmq-queue-durable      Declare queue as durable\n");
  printf("      --rabbitmq-queue-transient    Declare queue as transient\n");
  printf(
      "      --read-timeout-seconds <sec>  Consume loop timeout in seconds\n");
  printf(
      "      --prefetch-count <count>      AMQP prefetch count (0..65535)\n");
  printf("      --auto-ack                    Enable automatic "
         "acknowledgements\n");
  printf("      --manual-ack                  Ack each message after "
         "processing\n");
  printf("  -h, --help                        Show this help message\n");
  printf("\n");
  printf("TOML keys:\n");
  printf("  rabbitmq.host\n");
  printf("  rabbitmq.port\n");
  printf("  rabbitmq.username\n");
  printf("  rabbitmq.password\n");
  printf("  rabbitmq.vhost\n");
  printf("  rabbitmq.queue\n");
  printf("  rabbitmq.queue_durable\n");
  printf("  rabbitmq.channel\n");
  printf("  rabbitmq.heartbeat_seconds\n");
  printf("  rabbitmq_consumer.read_timeout_seconds\n");
  printf("  rabbitmq_consumer.prefetch_count\n");
  printf("  rabbitmq_consumer.auto_ack\n");
  printf("  logging.level (trace|debug|info|warn|error|fatal)\n");
  printf("  logging.color\n");
}

[[nodiscard]] static char *app_string_duplicate(const char *text) {
  if (text == nullptr) {
    return nullptr;
  }

  size_t len = strlen(text);
  size_t allocation_length = 0;
  if (ckd_add(&allocation_length, len, (size_t)1)) {
    return nullptr;
  }

  char *copy = calloc(allocation_length, sizeof(char));
  if (copy == nullptr) {
    return nullptr;
  }

  memcpy(copy, text, len + 1);
  return copy;
}

[[nodiscard]] static bool app_replace_string(const char **target, char **owned,
                                             const char *value) {
  if (target == nullptr || owned == nullptr || value == nullptr) {
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

void app_config_set_defaults(app_config_t *config) {
  if (config == nullptr) {
    return;
  }

  config->rabbitmq_host = APP_DEFAULT_RABBITMQ_HOST;
  config->rabbitmq_port = APP_DEFAULT_RABBITMQ_PORT;
  config->rabbitmq_username = APP_DEFAULT_RABBITMQ_USERNAME;
  config->rabbitmq_password = APP_DEFAULT_RABBITMQ_PASSWORD;
  config->rabbitmq_vhost = APP_DEFAULT_RABBITMQ_VHOST;
  config->rabbitmq_queue = APP_DEFAULT_RABBITMQ_QUEUE;
  config->rabbitmq_queue_durable = APP_DEFAULT_RABBITMQ_QUEUE_DURABLE;
  config->rabbitmq_channel = APP_DEFAULT_RABBITMQ_CHANNEL;
  config->rabbitmq_heartbeat_seconds = APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS;

  config->read_timeout_seconds = APP_DEFAULT_READ_TIMEOUT_SECONDS;
  config->prefetch_count = APP_DEFAULT_PREFETCH_COUNT;
  config->auto_ack = APP_DEFAULT_AUTO_ACK;
  config->log_level = APP_DEFAULT_LOG_LEVEL;
  config->log_color = APP_DEFAULT_LOG_COLOR;
}

void app_config_cleanup(app_config_t *config) {
  if (config == nullptr) {
    return;
  }

  free(config->rabbitmq_host_owned);
  free(config->rabbitmq_username_owned);
  free(config->rabbitmq_password_owned);
  free(config->rabbitmq_vhost_owned);
  free(config->rabbitmq_queue_owned);

  config->rabbitmq_host_owned = nullptr;
  config->rabbitmq_username_owned = nullptr;
  config->rabbitmq_password_owned = nullptr;
  config->rabbitmq_vhost_owned = nullptr;
  config->rabbitmq_queue_owned = nullptr;
}

[[nodiscard]] static bool app_parse_uint16(const char *text, bool allow_zero,
                                           uint16_t *out_value) {
  if (text == nullptr || text[0] == '\0' || out_value == nullptr ||
      text[0] == '-') {
    return false;
  }

  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed > UINT16_MAX) {
    return false;
  }
  if (!allow_zero && parsed == 0) {
    return false;
  }

  *out_value = (uint16_t)parsed;
  return true;
}

[[nodiscard]] static bool app_parse_uint32(const char *text, uint32_t min_value,
                                           uint32_t max_value,
                                           uint32_t *out_value) {
  if (text == nullptr || text[0] == '\0' || out_value == nullptr ||
      text[0] == '-' || min_value > max_value) {
    return false;
  }

  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed < min_value ||
      parsed > max_value) {
    return false;
  }

  *out_value = (uint32_t)parsed;
  return true;
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

[[nodiscard]] static bool app_apply_toml_string(toml_datum_t root,
                                                const char *key,
                                                const char **target,
                                                char **owned) {
  if (root.type != TOML_TABLE || key == nullptr || target == nullptr ||
      owned == nullptr) {
    return false;
  }

  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }
  if (value.type != TOML_STRING) {
    ulog_error("Config key '%s' must be a string\n", key);
    return false;
  }

  if (!app_replace_string(target, owned, value.u.s)) {
    ulog_error("Out of memory while loading config key '%s'\n", key);
    return false;
  }

  return true;
}

/* Load the optional TOML layer over defaults; CLI remains higher priority. */
[[nodiscard]] bool app_load_toml_config(app_config_t *config,
                                        const app_cli_overrides_t *overrides) {
  if (config == nullptr || overrides == nullptr) {
    return false;
  }

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

  toml_datum_t rabbitmq_port = toml_seek(parsed.toptab, "rabbitmq.port");
  if (ok && rabbitmq_port.type != TOML_UNKNOWN) {
    if (rabbitmq_port.type != TOML_INT64 || rabbitmq_port.u.int64 <= 0 ||
        rabbitmq_port.u.int64 > UINT16_MAX) {
      ulog_error(
          "Config key 'rabbitmq.port' must be an integer in range 1..65535\n");
      ok = false;
    } else {
      config->rabbitmq_port = (uint16_t)rabbitmq_port.u.int64;
    }
  }

  toml_datum_t rabbitmq_queue_durable =
      toml_seek(parsed.toptab, "rabbitmq.queue_durable");
  if (ok && rabbitmq_queue_durable.type != TOML_UNKNOWN) {
    if (rabbitmq_queue_durable.type != TOML_BOOLEAN) {
      ulog_error("Config key 'rabbitmq.queue_durable' must be a boolean\n");
      ok = false;
    } else {
      config->rabbitmq_queue_durable = rabbitmq_queue_durable.u.boolean;
    }
  }

  toml_datum_t rabbitmq_channel = toml_seek(parsed.toptab, "rabbitmq.channel");
  if (ok && rabbitmq_channel.type != TOML_UNKNOWN) {
    if (rabbitmq_channel.type != TOML_INT64 || rabbitmq_channel.u.int64 <= 0 ||
        rabbitmq_channel.u.int64 > UINT16_MAX) {
      ulog_error("Config key 'rabbitmq.channel' must be an integer in "
                 "range 1..65535\n");
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
      ulog_error("Config key 'rabbitmq.heartbeat_seconds' must be an "
                 "integer in range 1..65535\n");
      ok = false;
    } else {
      config->rabbitmq_heartbeat_seconds =
          (uint16_t)rabbitmq_heartbeat_seconds.u.int64;
    }
  }

  toml_datum_t read_timeout_seconds =
      toml_seek(parsed.toptab, "rabbitmq_consumer.read_timeout_seconds");
  if (ok && read_timeout_seconds.type != TOML_UNKNOWN) {
    if (read_timeout_seconds.type != TOML_INT64 ||
        read_timeout_seconds.u.int64 <= 0 ||
        read_timeout_seconds.u.int64 > INT_MAX) {
      ulog_error(
          "Config key 'rabbitmq_consumer.read_timeout_seconds' must be an "
          "integer in range 1..%d\n",
          INT_MAX);
      ok = false;
    } else {
      config->read_timeout_seconds = (uint32_t)read_timeout_seconds.u.int64;
    }
  }

  toml_datum_t prefetch_count =
      toml_seek(parsed.toptab, "rabbitmq_consumer.prefetch_count");
  if (ok && prefetch_count.type != TOML_UNKNOWN) {
    if (prefetch_count.type != TOML_INT64 || prefetch_count.u.int64 < 0 ||
        prefetch_count.u.int64 > UINT16_MAX) {
      ulog_error("Config key 'rabbitmq_consumer.prefetch_count' must be "
                 "an integer in range 0..65535\n");
      ok = false;
    } else {
      config->prefetch_count = (uint16_t)prefetch_count.u.int64;
    }
  }

  toml_datum_t auto_ack =
      toml_seek(parsed.toptab, "rabbitmq_consumer.auto_ack");
  if (ok && auto_ack.type != TOML_UNKNOWN) {
    if (auto_ack.type != TOML_BOOLEAN) {
      ulog_error("Config key 'rabbitmq_consumer.auto_ack' must be a boolean\n");
      ok = false;
    } else {
      config->auto_ack = auto_ack.u.boolean;
    }
  }

  toml_datum_t log_level = toml_seek(parsed.toptab, "logging.level");
  if (ok && log_level.type != TOML_UNKNOWN) {
    if (log_level.type != TOML_STRING) {
      ulog_error("Config key 'logging.level' must be a string\n");
      ok = false;
    } else if (!app_parse_log_level(log_level.u.s, &config->log_level)) {
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

/* Store borrowed argv views without transferring their ownership. */
[[nodiscard]] bool app_parse_cli(int argc, char *argv[],
                                 app_cli_overrides_t *overrides) {
  if (overrides == nullptr) {
    return false;
  }

  *overrides = (app_cli_overrides_t){
      .config_path = APP_DEFAULT_CONFIG_PATH,
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

    if (strcmp(arg, "--rabbitmq-host") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_host = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-port") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_port_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-user") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_username = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-password") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_password = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-vhost") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_vhost = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-queue") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->rabbitmq_queue = argv[++i];
      continue;
    }

    if (strcmp(arg, "--rabbitmq-queue-durable") == 0) {
      overrides->rabbitmq_queue_durable_set = true;
      overrides->rabbitmq_queue_durable = true;
      continue;
    }

    if (strcmp(arg, "--rabbitmq-queue-transient") == 0) {
      overrides->rabbitmq_queue_durable_set = true;
      overrides->rabbitmq_queue_durable = false;
      continue;
    }

    if (strcmp(arg, "--read-timeout-seconds") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->read_timeout_seconds_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "--prefetch-count") == 0) {
      if (i + 1 >= argc) {
        ulog_error("Missing value for %s\n", arg);
        return false;
      }
      overrides->prefetch_count_text = argv[++i];
      continue;
    }

    if (strcmp(arg, "--auto-ack") == 0) {
      overrides->auto_ack_set = true;
      overrides->auto_ack = true;
      continue;
    }

    if (strcmp(arg, "--manual-ack") == 0) {
      overrides->auto_ack_set = true;
      overrides->auto_ack = false;
      continue;
    }

    ulog_error("Unrecognized argument: %s\n", arg);
    return false;
  }

  return true;
}

/* Commit validated CLI values as the final configuration layer. */
[[nodiscard]] bool
app_apply_cli_overrides(app_config_t *config,
                        const app_cli_overrides_t *overrides) {
  if (config == nullptr || overrides == nullptr) {
    return false;
  }

  if (overrides->rabbitmq_host != nullptr &&
      !app_replace_string(&config->rabbitmq_host, &config->rabbitmq_host_owned,
                          overrides->rabbitmq_host)) {
    ulog_error("Out of memory while applying --rabbitmq-host\n");
    return false;
  }

  if (overrides->rabbitmq_username != nullptr &&
      !app_replace_string(&config->rabbitmq_username,
                          &config->rabbitmq_username_owned,
                          overrides->rabbitmq_username)) {
    ulog_error("Out of memory while applying --rabbitmq-user\n");
    return false;
  }

  if (overrides->rabbitmq_password != nullptr &&
      !app_replace_string(&config->rabbitmq_password,
                          &config->rabbitmq_password_owned,
                          overrides->rabbitmq_password)) {
    ulog_error("Out of memory while applying --rabbitmq-password\n");
    return false;
  }

  if (overrides->rabbitmq_vhost != nullptr &&
      !app_replace_string(&config->rabbitmq_vhost,
                          &config->rabbitmq_vhost_owned,
                          overrides->rabbitmq_vhost)) {
    ulog_error("Out of memory while applying --rabbitmq-vhost\n");
    return false;
  }

  if (overrides->rabbitmq_queue != nullptr &&
      !app_replace_string(&config->rabbitmq_queue,
                          &config->rabbitmq_queue_owned,
                          overrides->rabbitmq_queue)) {
    ulog_error("Out of memory while applying --rabbitmq-queue\n");
    return false;
  }

  if (overrides->rabbitmq_port_text != nullptr &&
      !app_parse_uint16(overrides->rabbitmq_port_text, false,
                        &config->rabbitmq_port)) {
    ulog_error("Invalid --rabbitmq-port value '%s' (expected 1..65535)\n",
               overrides->rabbitmq_port_text);
    return false;
  }

  if (overrides->read_timeout_seconds_text != nullptr &&
      !app_parse_uint32(overrides->read_timeout_seconds_text, 1,
                        (uint32_t)INT_MAX, &config->read_timeout_seconds)) {
    ulog_error("Invalid --read-timeout-seconds value '%s' (expected 1..%d)\n",
               overrides->read_timeout_seconds_text, INT_MAX);
    return false;
  }

  if (overrides->prefetch_count_text != nullptr) {
    uint16_t parsed_prefetch = 0;
    if (!app_parse_uint16(overrides->prefetch_count_text, true,
                          &parsed_prefetch)) {
      ulog_error("Invalid --prefetch-count value '%s' (expected 0..65535)\n",
                 overrides->prefetch_count_text);
      return false;
    }
    config->prefetch_count = parsed_prefetch;
  }

  if (overrides->rabbitmq_queue_durable_set) {
    config->rabbitmq_queue_durable = overrides->rabbitmq_queue_durable;
  }

  if (overrides->auto_ack_set) {
    config->auto_ack = overrides->auto_ack;
  }

  return true;
}
