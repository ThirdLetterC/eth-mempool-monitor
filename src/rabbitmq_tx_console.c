#define _POSIX_C_SOURCE 200809L

#include "parson/parson.h"
#include "rabbitmq/amqp.h"
#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>

typedef struct app_config app_config_t;
struct app_config {
  const char *rabbitmq_host;
  uint16_t rabbitmq_port;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;
  bool rabbitmq_queue_durable;
  uint16_t rabbitmq_channel;
  uint16_t rabbitmq_heartbeat_seconds;

  uint32_t read_timeout_seconds;
  uint16_t prefetch_count;
  bool auto_ack;
  ulog_level log_level;
  bool log_color;

  char *rabbitmq_host_owned;
  char *rabbitmq_username_owned;
  char *rabbitmq_password_owned;
  char *rabbitmq_vhost_owned;
  char *rabbitmq_queue_owned;
};

typedef struct app_cli_overrides app_cli_overrides_t;
struct app_cli_overrides {
  const char *config_path;
  bool config_path_set;

  const char *rabbitmq_host;
  const char *rabbitmq_port_text;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;
  const char *read_timeout_seconds_text;
  const char *prefetch_count_text;

  bool rabbitmq_queue_durable_set;
  bool rabbitmq_queue_durable;
  bool auto_ack_set;
  bool auto_ack;

  bool show_help;
};

typedef struct app_rabbitmq_consumer app_rabbitmq_consumer_t;
struct app_rabbitmq_consumer {
  amqp_connection_state_t connection;
  amqp_channel_t channel;
  bool logged_in;
  bool channel_open;
};

static constexpr char APP_DEFAULT_CONFIG_PATH[] = "config.toml";
static constexpr char APP_DEFAULT_RABBITMQ_HOST[] = "127.0.0.1";
static constexpr uint16_t APP_DEFAULT_RABBITMQ_PORT = 5672;
static constexpr char APP_DEFAULT_RABBITMQ_USERNAME[] = "guest";
static constexpr char APP_DEFAULT_RABBITMQ_PASSWORD[] = "guest";
static constexpr char APP_DEFAULT_RABBITMQ_VHOST[] = "/";
static constexpr char APP_DEFAULT_RABBITMQ_QUEUE[] = "monitored_transactions";
static constexpr bool APP_DEFAULT_RABBITMQ_QUEUE_DURABLE = true;
static constexpr uint16_t APP_DEFAULT_RABBITMQ_CHANNEL = 1;
static constexpr uint16_t APP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS = 30;

static constexpr uint32_t APP_DEFAULT_READ_TIMEOUT_SECONDS = 1;
static constexpr uint16_t APP_DEFAULT_PREFETCH_COUNT = 200;
static constexpr bool APP_DEFAULT_AUTO_ACK = false;
static constexpr ulog_level APP_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
static constexpr bool APP_DEFAULT_LOG_COLOR = true;

static constexpr uint64_t APP_WEI_PER_GWEI = 1'000'000'000ULL;
static constexpr uint64_t APP_WEI_PER_ETH = 1'000'000'000'000'000'000ULL;

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
    ulog_error("Failed to initialize signal handler mask\n");
    return false;
  }

  if (sigaction(SIGINT, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGINT handler: %s\n", strerror(errno));
    return false;
  }
  if (sigaction(SIGTERM, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGTERM handler: %s\n", strerror(errno));
    return false;
  }

  return true;
}

static void app_print_usage(const char *program_name) {
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
  char *copy = calloc(len + 1, sizeof(char));
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

static void app_config_set_defaults(app_config_t *config) {
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

static void app_config_cleanup(app_config_t *config) {
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

[[nodiscard]] static bool
app_load_toml_config(app_config_t *config,
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

[[nodiscard]] static bool app_parse_cli(int argc, char *argv[],
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

[[nodiscard]] static bool
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

static void app_log_rpc_failure(const char *action, amqp_rpc_reply_t reply) {
  if (action == nullptr) {
    action = "operation";
  }

  if (reply.reply_type == AMQP_RESPONSE_NONE) {
    ulog_error("RabbitMQ %s failed: missing RPC reply\n", action);
    return;
  }
  if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: %s\n", action,
               amqp_error_string2(reply.library_error));
    return;
  }
  if (reply.reply_type != AMQP_RESPONSE_SERVER_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: unexpected reply type=%d\n", action,
               reply.reply_type);
    return;
  }

  if (reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
    amqp_connection_close_t *connection_close =
        (amqp_connection_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server connection close %u (%.*s)\n",
               action, connection_close->reply_code,
               (int)connection_close->reply_text.len,
               (char *)connection_close->reply_text.bytes);
    return;
  }

  if (reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
    amqp_channel_close_t *channel_close =
        (amqp_channel_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: server channel close %u (%.*s)\n", action,
               channel_close->reply_code, (int)channel_close->reply_text.len,
               (char *)channel_close->reply_text.bytes);
    return;
  }

  ulog_error("RabbitMQ %s failed: server exception method=0x%08X\n", action,
             reply.reply.id);
}

[[nodiscard]] static bool
app_expect_normal_reply(amqp_connection_state_t connection,
                        const char *action) {
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection);
  if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
    return true;
  }

  app_log_rpc_failure(action, reply);
  return false;
}

static void app_rabbitmq_disconnect(app_rabbitmq_consumer_t *consumer) {
  if (consumer == nullptr || consumer->connection == nullptr) {
    return;
  }

  if (consumer->channel_open) {
    (void)amqp_channel_close(consumer->connection, consumer->channel,
                             AMQP_REPLY_SUCCESS);
    consumer->channel_open = false;
  }

  if (consumer->logged_in) {
    (void)amqp_connection_close(consumer->connection, AMQP_REPLY_SUCCESS);
    consumer->logged_in = false;
  }

  int destroy_status = amqp_destroy_connection(consumer->connection);
  if (destroy_status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ destroy_connection returned: %s\n",
               amqp_error_string2(destroy_status));
  }

  consumer->connection = nullptr;
}

[[nodiscard]] static bool
app_rabbitmq_connect(const app_config_t *config,
                     app_rabbitmq_consumer_t *consumer) {
  if (config == nullptr || consumer == nullptr ||
      config->rabbitmq_host == nullptr || config->rabbitmq_port == 0 ||
      config->rabbitmq_username == nullptr ||
      config->rabbitmq_password == nullptr ||
      config->rabbitmq_vhost == nullptr || config->rabbitmq_queue == nullptr ||
      config->rabbitmq_channel == 0 ||
      config->rabbitmq_heartbeat_seconds == 0) {
    ulog_error("Invalid RabbitMQ configuration\n");
    return false;
  }

  *consumer = (app_rabbitmq_consumer_t){
      .channel = (amqp_channel_t)config->rabbitmq_channel,
  };

  consumer->connection = amqp_new_connection();
  if (consumer->connection == nullptr) {
    ulog_error("Failed to create RabbitMQ connection object\n");
    return false;
  }

  amqp_socket_t *socket = amqp_tcp_socket_new(consumer->connection);
  if (socket == nullptr) {
    ulog_error("Failed to create RabbitMQ TCP socket\n");
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  int socket_status = amqp_socket_open(socket, config->rabbitmq_host,
                                       (int)config->rabbitmq_port);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s\n",
               config->rabbitmq_host, config->rabbitmq_port,
               amqp_error_string2(socket_status));
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  amqp_rpc_reply_t login_reply = amqp_login(
      consumer->connection, config->rabbitmq_vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      (int)config->rabbitmq_heartbeat_seconds, AMQP_SASL_METHOD_PLAIN,
      config->rabbitmq_username, config->rabbitmq_password);
  if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    app_log_rpc_failure("login", login_reply);
    app_rabbitmq_disconnect(consumer);
    return false;
  }
  consumer->logged_in = true;

  if (amqp_channel_open(consumer->connection, consumer->channel) == nullptr ||
      !app_expect_normal_reply(consumer->connection, "channel open")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }
  consumer->channel_open = true;

  amqp_bytes_t queue_bytes = amqp_cstring_bytes(config->rabbitmq_queue);
  amqp_queue_declare_ok_t *queue_declare_ok = amqp_queue_declare(
      consumer->connection, consumer->channel, queue_bytes, false,
      config->rabbitmq_queue_durable, false, false, amqp_empty_table);
  if (queue_declare_ok == nullptr ||
      !app_expect_normal_reply(consumer->connection, "queue declare")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  if (config->prefetch_count > 0) {
    amqp_basic_qos_ok_t *qos_ok =
        amqp_basic_qos(consumer->connection, consumer->channel, 0,
                       config->prefetch_count, false);
    if (qos_ok == nullptr ||
        !app_expect_normal_reply(consumer->connection, "basic.qos")) {
      app_rabbitmq_disconnect(consumer);
      return false;
    }
  }

  amqp_basic_consume_ok_t *consume_ok = amqp_basic_consume(
      consumer->connection, consumer->channel, queue_bytes, amqp_empty_bytes,
      false, config->auto_ack, false, amqp_empty_table);
  if (consume_ok == nullptr ||
      !app_expect_normal_reply(consumer->connection, "basic.consume")) {
    app_rabbitmq_disconnect(consumer);
    return false;
  }

  ulog_info(
      "Connected to RabbitMQ at %s:%u (queue=%s durable=%s prefetch=%u "
      "auto_ack=%s)",
      config->rabbitmq_host, config->rabbitmq_port, config->rabbitmq_queue,
      config->rabbitmq_queue_durable ? "true" : "false",
      (unsigned)config->prefetch_count, config->auto_ack ? "true" : "false");

  return true;
}

[[nodiscard]] static const char *app_safe_string(const char *value) {
  return value != nullptr ? value : "(null)";
}

[[nodiscard]] static const char *
app_json_get_nullable_string(const JSON_Object *object, const char *name) {
  if (object == nullptr || name == nullptr) {
    return nullptr;
  }

  JSON_Value *value = json_object_get_value(object, name);
  if (value == nullptr) {
    return nullptr;
  }

  JSON_Value_Type type = json_value_get_type(value);
  if (type == JSONString) {
    return json_value_get_string(value);
  }
  if (type == JSONNull) {
    return nullptr;
  }

  return nullptr;
}

[[nodiscard]] static bool
app_json_get_boolean_or_default(const JSON_Object *object, const char *name,
                                bool default_value) {
  if (object == nullptr || name == nullptr) {
    return default_value;
  }

  JSON_Value *value = json_object_get_value(object, name);
  if (value == nullptr || json_value_get_type(value) != JSONBoolean) {
    return default_value;
  }

  return json_value_get_boolean(value) == JSONBooleanTrue;
}

[[nodiscard]] static bool app_hex_digit_value(char ch, uint8_t *out_digit) {
  if (out_digit == nullptr) {
    return false;
  }

  if (ch >= '0' && ch <= '9') {
    *out_digit = (uint8_t)(ch - '0');
    return true;
  }
  if (ch >= 'a' && ch <= 'f') {
    *out_digit = (uint8_t)(10 + ch - 'a');
    return true;
  }
  if (ch >= 'A' && ch <= 'F') {
    *out_digit = (uint8_t)(10 + ch - 'A');
    return true;
  }

  return false;
}

[[nodiscard]] static bool app_parse_hex_u64(const char *text,
                                            uint64_t *out_value) {
  if (text == nullptr || out_value == nullptr || text[0] != '0' ||
      (text[1] != 'x' && text[1] != 'X')) {
    return false;
  }

  if (text[2] == '\0') {
    *out_value = 0;
    return true;
  }

  uint64_t value = 0;
  for (size_t i = 2; text[i] != '\0'; ++i) {
    uint8_t digit = 0;
    if (!app_hex_digit_value(text[i], &digit)) {
      return false;
    }

    if (value > (UINT64_MAX - digit) / 16U) {
      return false;
    }
    value = value * 16U + digit;
  }

  *out_value = value;
  return true;
}

static void app_format_scaled_u64(uint64_t value, uint64_t scale,
                                  unsigned fractional_digits, char *buffer,
                                  size_t buffer_capacity) {
  if (buffer == nullptr || buffer_capacity == 0 || scale == 0) {
    return;
  }

  uint64_t integer_part = value / scale;
  uint64_t fractional_part = value % scale;
  if (fractional_digits == 0 || fractional_part == 0) {
    (void)snprintf(buffer, buffer_capacity, "%" PRIu64, integer_part);
    return;
  }

  char fractional_text[32] = {0};
  (void)snprintf(fractional_text, sizeof(fractional_text), "%0*" PRIu64,
                 (int)fractional_digits, fractional_part);

  size_t end = strlen(fractional_text);
  while (end > 0 && fractional_text[end - 1] == '0') {
    fractional_text[end - 1] = '\0';
    --end;
  }

  if (end == 0) {
    (void)snprintf(buffer, buffer_capacity, "%" PRIu64, integer_part);
    return;
  }

  (void)snprintf(buffer, buffer_capacity, "%" PRIu64 ".%s", integer_part,
                 fractional_text);
}

static void app_print_hex_line(const char *label, const char *value) {
  if (value == nullptr) {
    printf("%-24s %s\n", label, "(null)");
    return;
  }

  uint64_t parsed = 0;
  if (app_parse_hex_u64(value, &parsed)) {
    printf("%-24s %s (%" PRIu64 ")\n", label, value, parsed);
  } else {
    printf("%-24s %s\n", label, value);
  }
}

static void app_print_wei_line(const char *label, const char *value,
                               bool as_eth) {
  if (value == nullptr) {
    printf("%-24s %s\n", label, "(null)");
    return;
  }

  uint64_t parsed = 0;
  if (!app_parse_hex_u64(value, &parsed)) {
    printf("%-24s %s\n", label, value);
    return;
  }

  char formatted[96] = {0};
  if (as_eth) {
    app_format_scaled_u64(parsed, APP_WEI_PER_ETH, 18, formatted,
                          sizeof(formatted));
    printf("%-24s %s (%" PRIu64 " wei, %s ETH)\n", label, value, parsed,
           formatted);
  } else {
    app_format_scaled_u64(parsed, APP_WEI_PER_GWEI, 9, formatted,
                          sizeof(formatted));
    printf("%-24s %s (%" PRIu64 " wei, %s Gwei)\n", label, value, parsed,
           formatted);
  }
}

static void app_print_transaction_summary(const JSON_Object *event) {
  if (event == nullptr) {
    return;
  }

  const char *hash = app_json_get_nullable_string(event, "hash");
  const char *from = app_json_get_nullable_string(event, "from");
  const char *to = app_json_get_nullable_string(event, "to");
  bool from_monitored =
      app_json_get_boolean_or_default(event, "from_monitored", false);
  bool to_monitored =
      app_json_get_boolean_or_default(event, "to_monitored", false);

  const JSON_Object *transaction = json_object_get_object(event, "transaction");
  if (transaction == nullptr) {
    JSON_Value *tx_value = json_object_get_value(event, "transaction");
    if (tx_value != nullptr && json_value_get_type(tx_value) == JSONObject) {
      transaction = json_value_get_object(tx_value);
    }
  }

  if (hash == nullptr && transaction != nullptr) {
    hash = app_json_get_nullable_string(transaction, "hash");
  }

  printf("\n============================================================\n");
  printf("Monitored Transaction\n");
  printf("============================================================\n");
  printf("%-24s %s\n", "Hash", app_safe_string(hash));
  printf("%-24s %s [%s]\n", "From", app_safe_string(from),
         from_monitored ? "monitored" : "not monitored");
  printf("%-24s %s [%s]\n", "To", app_safe_string(to),
         to_monitored ? "monitored" : "not monitored");

  if (transaction == nullptr) {
    printf("%-24s %s\n", "Transaction", "(missing or invalid)");
    fflush(stdout);
    return;
  }

  const char *chain_id = app_json_get_nullable_string(transaction, "chainId");
  const char *nonce = app_json_get_nullable_string(transaction, "nonce");
  const char *tx_type = app_json_get_nullable_string(transaction, "type");
  const char *tx_to = app_json_get_nullable_string(transaction, "to");
  const char *input = app_json_get_nullable_string(transaction, "input");
  const char *gas = app_json_get_nullable_string(transaction, "gas");
  const char *gas_price = app_json_get_nullable_string(transaction, "gasPrice");
  const char *max_fee_per_gas =
      app_json_get_nullable_string(transaction, "maxFeePerGas");
  const char *max_priority_fee_per_gas =
      app_json_get_nullable_string(transaction, "maxPriorityFeePerGas");
  const char *value = app_json_get_nullable_string(transaction, "value");

  app_print_hex_line("Chain ID", chain_id);
  app_print_hex_line("Nonce", nonce);
  printf("%-24s %s\n", "Type", app_safe_string(tx_type));
  printf("%-24s %s\n", "Tx To", app_safe_string(tx_to));
  app_print_hex_line("Gas Limit", gas);
  app_print_wei_line("Gas Price", gas_price, false);
  app_print_wei_line("Max Fee Per Gas", max_fee_per_gas, false);
  app_print_wei_line("Max Priority Fee", max_priority_fee_per_gas, false);
  app_print_wei_line("Value", value, true);
  printf("%-24s %s\n", "Input", app_safe_string(input));

  fflush(stdout);
}

static void app_handle_payload(const void *body, size_t body_length) {
  if (body_length > 0 && body == nullptr) {
    ulog_error("Skipping message with null payload pointer\n");
    return;
  }

  char *payload = calloc(body_length + 1, sizeof(char));
  if (payload == nullptr) {
    ulog_error("Out of memory while copying RabbitMQ payload (%zu bytes)\n",
               body_length);
    return;
  }

  if (body_length > 0) {
    memcpy(payload, body, body_length);
  }

  JSON_Value *root = json_parse_string(payload);
  if (root == nullptr) {
    ulog_error("Failed to parse RabbitMQ message as JSON (%zu bytes): %s\n",
               body_length, payload);
    free(payload);
    return;
  }

  JSON_Object *event = json_value_get_object(root);
  if (event == nullptr) {
    ulog_error("RabbitMQ message JSON is not an object: %s\n", payload);
    json_value_free(root);
    free(payload);
    return;
  }

  app_print_transaction_summary(event);

  json_value_free(root);
  free(payload);
}

[[nodiscard]] static bool app_consume_loop(app_rabbitmq_consumer_t *consumer,
                                           const app_config_t *config) {
  if (consumer == nullptr || config == nullptr ||
      consumer->connection == nullptr) {
    return false;
  }

  struct timeval timeout = {
      .tv_sec = (time_t)config->read_timeout_seconds,
      .tv_usec = 0,
  };

  while (!app_is_shutdown_requested()) {
    amqp_maybe_release_buffers(consumer->connection);

    amqp_envelope_t envelope = {0};
    amqp_rpc_reply_t reply =
        amqp_consume_message(consumer->connection, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      app_handle_payload(envelope.message.body.bytes,
                         envelope.message.body.len);

      if (!config->auto_ack) {
        int ack_status = amqp_basic_ack(consumer->connection, consumer->channel,
                                        envelope.delivery_tag, false);
        if (ack_status != AMQP_STATUS_OK) {
          ulog_error("RabbitMQ ack failed: %s\n",
                     amqp_error_string2(ack_status));
          amqp_destroy_envelope(&envelope);
          return false;
        }
      }

      amqp_destroy_envelope(&envelope);
      continue;
    }

    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      continue;
    }

    app_log_rpc_failure("consume message", reply);
    return false;
  }

  return true;
}

int main(int argc, char *argv[]) {
  if (!app_apply_log_style_defaults()) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  app_cli_overrides_t overrides = {0};
  if (!app_parse_cli(argc, argv, &overrides)) {
    app_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    app_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  app_config_t config = {0};
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
    return EXIT_FAILURE;
  }

  ulog_info("Starting RabbitMQ transaction console");
  ulog_info("RabbitMQ endpoint: %s:%u vhost=%s queue=%s durable=%s channel=%u "
            "heartbeat=%u sec",
            config.rabbitmq_host, config.rabbitmq_port, config.rabbitmq_vhost,
            config.rabbitmq_queue,
            config.rabbitmq_queue_durable ? "true" : "false",
            (unsigned)config.rabbitmq_channel,
            (unsigned)config.rabbitmq_heartbeat_seconds);
  ulog_info("Consumer settings: timeout=%u sec prefetch=%u auto_ack=%s",
            (unsigned)config.read_timeout_seconds,
            (unsigned)config.prefetch_count,
            config.auto_ack ? "true" : "false");

  app_rabbitmq_consumer_t consumer = {0};
  if (!app_rabbitmq_connect(&config, &consumer)) {
    app_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  ok = app_consume_loop(&consumer, &config);

  app_rabbitmq_disconnect(&consumer);
  app_config_cleanup(&config);

  if (app_is_shutdown_requested()) {
    ulog_info("Received %s, shutting down",
              app_signal_name((int)app_shutdown_signal));
    return EXIT_SUCCESS;
  }

  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
