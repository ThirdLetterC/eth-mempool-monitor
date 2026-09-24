#include "websocket-client/http_transmitter_internal.h"
#include "toml/toml.h"
#include "ulog/ulog.h"

#include <errno.h>
#include <limits.h>
#include <stdckdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

constexpr char HTTP_DEFAULT_CONFIG_PATH[] = "conf/config.toml";
constexpr char HTTP_DEFAULT_RABBITMQ_HOST[] = "127.0.0.1";
constexpr uint16_t HTTP_DEFAULT_RABBITMQ_PORT = 5'672;
constexpr char HTTP_DEFAULT_RABBITMQ_USERNAME[] = "guest";
constexpr char HTTP_DEFAULT_RABBITMQ_PASSWORD[] = "guest";
constexpr char HTTP_DEFAULT_RABBITMQ_VHOST[] = "/";
constexpr char HTTP_DEFAULT_RABBITMQ_QUEUE[] = "monitored_transactions";
constexpr bool HTTP_DEFAULT_RABBITMQ_QUEUE_DURABLE = true;
constexpr uint16_t HTTP_DEFAULT_RABBITMQ_CHANNEL = 1;
constexpr uint16_t HTTP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS = 30;
constexpr uint32_t HTTP_DEFAULT_READ_TIMEOUT_SECONDS = 1;
constexpr uint16_t HTTP_DEFAULT_PREFETCH_COUNT = 1;
constexpr uint32_t HTTP_DEFAULT_CONNECT_TIMEOUT_MS = 5'000;
constexpr uint32_t HTTP_DEFAULT_REQUEST_TIMEOUT_MS = 15'000;
constexpr uint32_t HTTP_DEFAULT_MAX_ATTEMPTS = 3;
constexpr uint32_t HTTP_DEFAULT_INITIAL_BACKOFF_MS = 500;
constexpr uint32_t HTTP_DEFAULT_MAX_BACKOFF_MS = 5'000;
constexpr uint32_t HTTP_MAX_ATTEMPTS_LIMIT = 100;
constexpr uint16_t HTTP_DEFAULT_PARALLEL_REQUESTS = 1;
constexpr uint16_t HTTP_MAX_PARALLEL_REQUESTS = 256;
constexpr ulog_level HTTP_DEFAULT_LOG_LEVEL = ULOG_LEVEL_INFO;
constexpr bool HTTP_DEFAULT_LOG_COLOR = true;

static void http_secure_clear(char *text) {
  if (text == nullptr) {
    return;
  }
  volatile unsigned char *cursor = (volatile unsigned char *)text;
  size_t length = strlen(text);
  while (length > 0) {
    *cursor = 0;
    ++cursor;
    --length;
  }
}

[[nodiscard]] static char *http_string_duplicate(const char *text) {
  if (text == nullptr) {
    return nullptr;
  }
  size_t length = strlen(text);
  size_t allocation_length = 0;
  if (ckd_add(&allocation_length, length, (size_t)1)) {
    return nullptr;
  }
  char *copy = calloc(allocation_length, sizeof(char));
  if (copy == nullptr) {
    return nullptr;
  }
  memcpy(copy, text, allocation_length);
  return copy;
}

[[nodiscard]] static bool http_replace_string(const char **target, char **owned,
                                              const char *value) {
  if (target == nullptr || owned == nullptr || value == nullptr) {
    return false;
  }
  char *copy = http_string_duplicate(value);
  if (copy == nullptr) {
    return false;
  }
  free(*owned);
  *owned = copy;
  *target = copy;
  return true;
}

void http_transmitter_config_set_defaults(http_transmitter_config_t *config) {
  if (config == nullptr) {
    return;
  }
  *config = (http_transmitter_config_t){
      .rabbitmq_host = HTTP_DEFAULT_RABBITMQ_HOST,
      .rabbitmq_port = {.value = HTTP_DEFAULT_RABBITMQ_PORT},
      .rabbitmq_username = HTTP_DEFAULT_RABBITMQ_USERNAME,
      .rabbitmq_password = HTTP_DEFAULT_RABBITMQ_PASSWORD,
      .rabbitmq_vhost = HTTP_DEFAULT_RABBITMQ_VHOST,
      .rabbitmq_queue = HTTP_DEFAULT_RABBITMQ_QUEUE,
      .rabbitmq_queue_durable = HTTP_DEFAULT_RABBITMQ_QUEUE_DURABLE,
      .rabbitmq_channel = {.value = HTTP_DEFAULT_RABBITMQ_CHANNEL},
      .rabbitmq_heartbeat = {.value = HTTP_DEFAULT_RABBITMQ_HEARTBEAT_SECONDS},
      .read_timeout = {.value = HTTP_DEFAULT_READ_TIMEOUT_SECONDS},
      .prefetch_count = {.value = HTTP_DEFAULT_PREFETCH_COUNT},
      .connect_timeout = {.value = HTTP_DEFAULT_CONNECT_TIMEOUT_MS},
      .request_timeout = {.value = HTTP_DEFAULT_REQUEST_TIMEOUT_MS},
      .max_attempts = HTTP_DEFAULT_MAX_ATTEMPTS,
      .initial_backoff = {.value = HTTP_DEFAULT_INITIAL_BACKOFF_MS},
      .max_backoff = {.value = HTTP_DEFAULT_MAX_BACKOFF_MS},
      .parallel_requests = {.value = HTTP_DEFAULT_PARALLEL_REQUESTS},
      .log_level = HTTP_DEFAULT_LOG_LEVEL,
      .log_color = HTTP_DEFAULT_LOG_COLOR,
  };
}

void http_transmitter_config_cleanup(http_transmitter_config_t *config) {
  if (config == nullptr) {
    return;
  }
  http_secure_clear(config->rabbitmq_password_owned);
  http_secure_clear(config->bearer_token_owned);
  free(config->rabbitmq_host_owned);
  free(config->rabbitmq_username_owned);
  free(config->rabbitmq_password_owned);
  free(config->rabbitmq_vhost_owned);
  free(config->rabbitmq_queue_owned);
  free(config->webhook_url_owned);
  free(config->bearer_token_env_owned);
  free(config->bearer_token_owned);
  *config = (http_transmitter_config_t){0};
}

void http_transmitter_print_usage(const char *program_name) {
  printf("Usage: %s [options]\n\n", program_name);
  printf("  -c, --config <file>                 TOML config path\n");
  printf("      --rabbitmq-host <host>          RabbitMQ host\n");
  printf("      --rabbitmq-port <port>          RabbitMQ port\n");
  printf("      --rabbitmq-user <user>          RabbitMQ username\n");
  printf("      --rabbitmq-password <pass>      RabbitMQ password\n");
  printf("      --rabbitmq-vhost <vhost>        RabbitMQ virtual host\n");
  printf("      --rabbitmq-queue <name>         RabbitMQ queue\n");
  printf("      --rabbitmq-queue-durable        Declare a durable queue\n");
  printf("      --rabbitmq-queue-transient      Declare a transient queue\n");
  printf("      --read-timeout-seconds <sec>    RabbitMQ read timeout\n");
  printf("      --prefetch-count <count>        RabbitMQ prefetch count\n");
  printf("      --webhook-url <url>             HTTP(S) webhook URL\n");
  printf(
      "      --webhook-bearer-token-env <n>  Bearer-token environment name\n");
  printf("      --webhook-connect-timeout-ms <ms>\n");
  printf("      --webhook-timeout-ms <ms>\n");
  printf("      --webhook-max-attempts <count>\n");
  printf("      --webhook-initial-backoff-ms <ms>\n");
  printf("      --webhook-max-backoff-ms <ms>\n");
  printf("      --webhook-parallel-requests <count>\n");
  printf("  -h, --help                          Show this help\n");
}

[[nodiscard]] static bool http_parse_u32(const char *text, uint32_t minimum,
                                         uint32_t maximum,
                                         uint32_t *out_value) {
  if (text == nullptr || text[0] == '\0' || text[0] == '-' ||
      out_value == nullptr || minimum > maximum) {
    return false;
  }
  errno = 0;
  char *end = nullptr;
  unsigned long parsed = strtoul(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed < minimum ||
      parsed > maximum) {
    return false;
  }
  *out_value = (uint32_t)parsed;
  return true;
}

[[nodiscard]] static bool http_parse_u16(const char *text, bool allow_zero,
                                         uint16_t *out_value) {
  uint32_t parsed = 0;
  if (!http_parse_u32(text, allow_zero ? 0U : 1U, UINT16_MAX, &parsed) ||
      out_value == nullptr) {
    return false;
  }
  *out_value = (uint16_t)parsed;
  return true;
}

[[nodiscard]] static bool http_parse_log_level(const char *text,
                                               ulog_level *out_level) {
  if (text == nullptr || out_level == nullptr) {
    return false;
  }
  const struct {
    const char *name;
    ulog_level level;
  } levels[] = {
      {"trace", ULOG_LEVEL_TRACE},  {"debug", ULOG_LEVEL_DEBUG},
      {"info", ULOG_LEVEL_INFO},    {"warn", ULOG_LEVEL_WARN},
      {"warning", ULOG_LEVEL_WARN}, {"error", ULOG_LEVEL_ERROR},
      {"fatal", ULOG_LEVEL_FATAL},
  };
  for (size_t i = 0; i < sizeof(levels) / sizeof(levels[0]); ++i) {
    if (strcasecmp(text, levels[i].name) == 0) {
      *out_level = levels[i].level;
      return true;
    }
  }
  return false;
}

[[nodiscard]] bool http_transmitter_apply_log_level(ulog_level level) {
  auto status = ulog_output_level_set_all(level);
  return status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED;
}

[[nodiscard]] bool http_transmitter_apply_log_color(bool enabled) {
  auto status = ulog_color_config(enabled);
  return status == ULOG_STATUS_OK || status == ULOG_STATUS_DISABLED;
}

[[nodiscard]] bool http_transmitter_apply_log_style_defaults() {
  auto prefix_status = ulog_prefix_config(false);
  auto time_status = ulog_time_config(false);
  return (prefix_status == ULOG_STATUS_OK ||
          prefix_status == ULOG_STATUS_DISABLED) &&
         (time_status == ULOG_STATUS_OK || time_status == ULOG_STATUS_DISABLED);
}

[[nodiscard]] static bool
http_apply_toml_string(toml_datum_t root, const char *key, const char **target,
                       char **owned, bool allow_empty) {
  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }
  if (value.type != TOML_STRING || (!allow_empty && value.u.s[0] == '\0')) {
    ulog_error("Config key '%s' must be a non-empty string", key);
    return false;
  }
  if (!http_replace_string(target, owned, value.u.s)) {
    ulog_error("Out of memory loading config key '%s'", key);
    return false;
  }
  return true;
}

[[nodiscard]] static bool http_apply_toml_u32(toml_datum_t root,
                                              const char *key, uint32_t minimum,
                                              uint32_t maximum,
                                              uint32_t *target) {
  toml_datum_t value = toml_seek(root, key);
  if (value.type == TOML_UNKNOWN) {
    return true;
  }
  if (value.type != TOML_INT64 || value.u.int64 < (int64_t)minimum ||
      value.u.int64 > (int64_t)maximum) {
    ulog_error("Config key '%s' must be an integer in range %u..%u", key,
               minimum, maximum);
    return false;
  }
  *target = (uint32_t)value.u.int64;
  return true;
}

[[nodiscard]] static bool
http_load_toml_impl(http_transmitter_config_t *config,
                    const http_transmitter_cli_overrides_t *overrides) {
  FILE *file = fopen(overrides->config_path, "rb");
  if (file == nullptr) {
    if (!overrides->config_path_set && errno == ENOENT) {
      return true;
    }
    ulog_error("Failed to open config file '%s': %s", overrides->config_path,
               strerror(errno));
    return false;
  }
  toml_result_t parsed = toml_parse_file(file);
  if (fclose(file) != 0) {
    ulog_error("Failed to close config file '%s'", overrides->config_path);
  }
  if (!parsed.ok) {
    ulog_error("Failed to parse config file '%s': %s", overrides->config_path,
               parsed.errmsg);
    toml_free(parsed);
    return false;
  }

  bool ok = true;
  ok = ok && http_apply_toml_string(parsed.toptab, "rabbitmq.host",
                                    &config->rabbitmq_host,
                                    &config->rabbitmq_host_owned, false);
  ok = ok && http_apply_toml_string(parsed.toptab, "rabbitmq.username",
                                    &config->rabbitmq_username,
                                    &config->rabbitmq_username_owned, true);
  ok = ok && http_apply_toml_string(parsed.toptab, "rabbitmq.password",
                                    &config->rabbitmq_password,
                                    &config->rabbitmq_password_owned, true);
  ok = ok && http_apply_toml_string(parsed.toptab, "rabbitmq.vhost",
                                    &config->rabbitmq_vhost,
                                    &config->rabbitmq_vhost_owned, false);
  ok = ok && http_apply_toml_string(parsed.toptab, "rabbitmq.queue",
                                    &config->rabbitmq_queue,
                                    &config->rabbitmq_queue_owned, false);
  ok = ok && http_apply_toml_string(parsed.toptab, "webhook.url",
                                    &config->webhook_url,
                                    &config->webhook_url_owned, false);
  ok = ok && http_apply_toml_string(parsed.toptab, "webhook.bearer_token_env",
                                    &config->bearer_token_env,
                                    &config->bearer_token_env_owned, false);

  uint32_t value = 0;
  if (ok && http_apply_toml_u32(parsed.toptab, "rabbitmq.port", 1, UINT16_MAX,
                                &value)) {
    if (toml_seek(parsed.toptab, "rabbitmq.port").type != TOML_UNKNOWN) {
      config->rabbitmq_port.value = (uint16_t)value;
    }
  } else {
    ok = false;
  }
  toml_datum_t durable = toml_seek(parsed.toptab, "rabbitmq.queue_durable");
  if (ok && durable.type != TOML_UNKNOWN) {
    if (durable.type != TOML_BOOLEAN) {
      ulog_error("Config key 'rabbitmq.queue_durable' must be a boolean");
      ok = false;
    } else {
      config->rabbitmq_queue_durable = durable.u.boolean;
    }
  }
#define APPLY_U32(KEY, MINIMUM, MAXIMUM, TARGET)                               \
  do {                                                                         \
    value = (TARGET);                                                          \
    if (ok && http_apply_toml_u32(parsed.toptab, (KEY), (MINIMUM), (MAXIMUM),  \
                                  &value)) {                                   \
      (TARGET) = (typeof_unqual(TARGET))value;                                 \
    } else {                                                                   \
      ok = false;                                                              \
    }                                                                          \
  } while (false)
  APPLY_U32("rabbitmq.channel", 1, UINT16_MAX, config->rabbitmq_channel.value);
  APPLY_U32("rabbitmq.heartbeat_seconds", 1, UINT16_MAX,
            config->rabbitmq_heartbeat.value);
  APPLY_U32("rabbitmq_consumer.read_timeout_seconds", 1, INT_MAX,
            config->read_timeout.value);
  APPLY_U32("rabbitmq_consumer.prefetch_count", 1, UINT16_MAX,
            config->prefetch_count.value);
  APPLY_U32("webhook.connect_timeout_ms", 1, INT_MAX,
            config->connect_timeout.value);
  APPLY_U32("webhook.request_timeout_ms", 1, INT_MAX,
            config->request_timeout.value);
  APPLY_U32("webhook.max_attempts", 1, HTTP_MAX_ATTEMPTS_LIMIT,
            config->max_attempts);
  APPLY_U32("webhook.initial_backoff_ms", 1, INT_MAX,
            config->initial_backoff.value);
  APPLY_U32("webhook.max_backoff_ms", 1, INT_MAX, config->max_backoff.value);
  APPLY_U32("webhook.parallel_requests", 1, HTTP_MAX_PARALLEL_REQUESTS,
            config->parallel_requests.value);
#undef APPLY_U32

  toml_datum_t level = toml_seek(parsed.toptab, "logging.level");
  if (ok && level.type != TOML_UNKNOWN &&
      (level.type != TOML_STRING ||
       !http_parse_log_level(level.u.s, &config->log_level))) {
    ulog_error("Config key 'logging.level' is invalid");
    ok = false;
  }
  toml_datum_t color = toml_seek(parsed.toptab, "logging.color");
  if (ok && color.type != TOML_UNKNOWN) {
    if (color.type != TOML_BOOLEAN) {
      ulog_error("Config key 'logging.color' must be a boolean");
      ok = false;
    } else {
      config->log_color = color.u.boolean;
    }
  }
  toml_free(parsed);
  return ok;
}

[[nodiscard]] app_config_status_t http_transmitter_load_toml_config(
    http_transmitter_config_t *config,
    const http_transmitter_cli_overrides_t *overrides) {
  if (config == nullptr || overrides == nullptr) {
    return APP_CONFIG_STATUS_INVALID_ARGUMENT;
  }
  return http_load_toml_impl(config, overrides) ? APP_CONFIG_STATUS_OK
                                                : APP_CONFIG_STATUS_LOAD_ERROR;
}

[[nodiscard]] static bool http_take_cli_value(int argc, char *argv[],
                                              int *index, const char **target) {
  if (*index + 1 >= argc) {
    return false;
  }
  ++*index;
  *target = argv[*index];
  return true;
}

[[nodiscard]] app_config_status_t
http_transmitter_parse_cli(int argc, char *argv[],
                           http_transmitter_cli_overrides_t *overrides) {
  if (argv == nullptr || overrides == nullptr) {
    return APP_CONFIG_STATUS_INVALID_ARGUMENT;
  }
  *overrides = (http_transmitter_cli_overrides_t){
      .config_path = HTTP_DEFAULT_CONFIG_PATH,
  };
  for (int i = 1; i < argc; ++i) {
    const char *arg = argv[i];
    if (strcmp(arg, "-h") == 0 || strcmp(arg, "--help") == 0) {
      overrides->show_help = true;
      return APP_CONFIG_STATUS_OK;
    }
#define CLI_VALUE(NAME, FIELD)                                                 \
  if (strcmp(arg, (NAME)) == 0) {                                              \
    if (!http_take_cli_value(argc, argv, &i, &(FIELD))) {                      \
      ulog_error("Missing value for %s", arg);                                 \
      return APP_CONFIG_STATUS_INVALID_ARGUMENT;                               \
    }                                                                          \
    continue;                                                                  \
  }
    if (strcmp(arg, "-c") == 0 || strcmp(arg, "--config") == 0) {
      if (!http_take_cli_value(argc, argv, &i, &overrides->config_path)) {
        ulog_error("Missing value for %s", arg);
        return APP_CONFIG_STATUS_INVALID_ARGUMENT;
      }
      overrides->config_path_set = true;
      continue;
    }
    CLI_VALUE("--rabbitmq-host", overrides->rabbitmq_host)
    CLI_VALUE("--rabbitmq-port", overrides->rabbitmq_port_text)
    CLI_VALUE("--rabbitmq-user", overrides->rabbitmq_username)
    CLI_VALUE("--rabbitmq-password", overrides->rabbitmq_password)
    CLI_VALUE("--rabbitmq-vhost", overrides->rabbitmq_vhost)
    CLI_VALUE("--rabbitmq-queue", overrides->rabbitmq_queue)
    CLI_VALUE("--read-timeout-seconds", overrides->read_timeout_seconds_text)
    CLI_VALUE("--prefetch-count", overrides->prefetch_count_text)
    CLI_VALUE("--webhook-url", overrides->webhook_url)
    CLI_VALUE("--webhook-bearer-token-env", overrides->bearer_token_env)
    CLI_VALUE("--webhook-connect-timeout-ms",
              overrides->connect_timeout_ms_text)
    CLI_VALUE("--webhook-timeout-ms", overrides->request_timeout_ms_text)
    CLI_VALUE("--webhook-max-attempts", overrides->max_attempts_text)
    CLI_VALUE("--webhook-initial-backoff-ms",
              overrides->initial_backoff_ms_text)
    CLI_VALUE("--webhook-max-backoff-ms", overrides->max_backoff_ms_text)
    CLI_VALUE("--webhook-parallel-requests", overrides->parallel_requests_text)
#undef CLI_VALUE
    if (strcmp(arg, "--rabbitmq-queue-durable") == 0 ||
        strcmp(arg, "--rabbitmq-queue-transient") == 0) {
      overrides->rabbitmq_queue_durable_set = true;
      overrides->rabbitmq_queue_durable =
          strcmp(arg, "--rabbitmq-queue-durable") == 0;
      continue;
    }
    ulog_error("Unrecognized argument: %s", arg);
    return APP_CONFIG_STATUS_INVALID_ARGUMENT;
  }
  return APP_CONFIG_STATUS_OK;
}

[[nodiscard]] static bool http_apply_cli_u32(const char *text,
                                             const char *option,
                                             uint32_t minimum, uint32_t maximum,
                                             uint32_t *target) {
  if (text == nullptr) {
    return true;
  }
  if (!http_parse_u32(text, minimum, maximum, target)) {
    ulog_error("Invalid %s value", option);
    return false;
  }
  return true;
}

[[nodiscard]] app_config_status_t http_transmitter_apply_cli_overrides(
    http_transmitter_config_t *config,
    const http_transmitter_cli_overrides_t *overrides) {
  if (config == nullptr || overrides == nullptr) {
    return APP_CONFIG_STATUS_INVALID_ARGUMENT;
  }
#define REPLACE_CLI(VALUE, TARGET, OWNED, OPTION)                              \
  do {                                                                         \
    if ((VALUE) != nullptr &&                                                  \
        !http_replace_string(&(TARGET), &(OWNED), (VALUE))) {                  \
      ulog_error("Out of memory applying %s", (OPTION));                       \
      return APP_CONFIG_STATUS_INVALID_VALUE;                                  \
    }                                                                          \
  } while (false)
  REPLACE_CLI(overrides->rabbitmq_host, config->rabbitmq_host,
              config->rabbitmq_host_owned, "--rabbitmq-host");
  REPLACE_CLI(overrides->rabbitmq_username, config->rabbitmq_username,
              config->rabbitmq_username_owned, "--rabbitmq-user");
  REPLACE_CLI(overrides->rabbitmq_password, config->rabbitmq_password,
              config->rabbitmq_password_owned, "--rabbitmq-password");
  REPLACE_CLI(overrides->rabbitmq_vhost, config->rabbitmq_vhost,
              config->rabbitmq_vhost_owned, "--rabbitmq-vhost");
  REPLACE_CLI(overrides->rabbitmq_queue, config->rabbitmq_queue,
              config->rabbitmq_queue_owned, "--rabbitmq-queue");
  REPLACE_CLI(overrides->webhook_url, config->webhook_url,
              config->webhook_url_owned, "--webhook-url");
  REPLACE_CLI(overrides->bearer_token_env, config->bearer_token_env,
              config->bearer_token_env_owned, "--webhook-bearer-token-env");
#undef REPLACE_CLI
  if (overrides->rabbitmq_port_text != nullptr &&
      !http_parse_u16(overrides->rabbitmq_port_text, false,
                      &config->rabbitmq_port.value)) {
    ulog_error("Invalid --rabbitmq-port value");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (overrides->prefetch_count_text != nullptr &&
      !http_parse_u16(overrides->prefetch_count_text, false,
                      &config->prefetch_count.value)) {
    ulog_error("Invalid --prefetch-count value");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  bool ok = true;
  ok = ok && http_apply_cli_u32(overrides->read_timeout_seconds_text,
                                "--read-timeout-seconds", 1, INT_MAX,
                                &config->read_timeout.value);
  ok = ok && http_apply_cli_u32(overrides->connect_timeout_ms_text,
                                "--webhook-connect-timeout-ms", 1, INT_MAX,
                                &config->connect_timeout.value);
  ok = ok && http_apply_cli_u32(overrides->request_timeout_ms_text,
                                "--webhook-timeout-ms", 1, INT_MAX,
                                &config->request_timeout.value);
  ok = ok && http_apply_cli_u32(overrides->max_attempts_text,
                                "--webhook-max-attempts", 1,
                                HTTP_MAX_ATTEMPTS_LIMIT, &config->max_attempts);
  ok = ok && http_apply_cli_u32(overrides->initial_backoff_ms_text,
                                "--webhook-initial-backoff-ms", 1, INT_MAX,
                                &config->initial_backoff.value);
  ok = ok && http_apply_cli_u32(overrides->max_backoff_ms_text,
                                "--webhook-max-backoff-ms", 1, INT_MAX,
                                &config->max_backoff.value);
  uint32_t parallel_requests = config->parallel_requests.value;
  ok = ok && http_apply_cli_u32(overrides->parallel_requests_text,
                                "--webhook-parallel-requests", 1,
                                HTTP_MAX_PARALLEL_REQUESTS, &parallel_requests);
  config->parallel_requests.value = (uint16_t)parallel_requests;
  if (!ok) {
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (overrides->rabbitmq_queue_durable_set) {
    config->rabbitmq_queue_durable = overrides->rabbitmq_queue_durable;
  }
  return APP_CONFIG_STATUS_OK;
}

[[nodiscard]] app_config_status_t
http_transmitter_finalize_config(http_transmitter_config_t *config) {
  if (config == nullptr || config->webhook_url == nullptr ||
      config->webhook_url[0] == '\0') {
    ulog_error("Required config key 'webhook.url' is missing");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (strncmp(config->webhook_url, "http://", 7) != 0 &&
      strncmp(config->webhook_url, "https://", 8) != 0) {
    ulog_error("Webhook URL must use http:// or https://");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (config->initial_backoff.value > config->max_backoff.value) {
    ulog_error("Webhook initial backoff must not exceed maximum backoff");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (config->parallel_requests.value > config->prefetch_count.value) {
    ulog_error("Webhook parallel requests must not exceed RabbitMQ prefetch "
               "count");
    return APP_CONFIG_STATUS_INVALID_VALUE;
  }
  if (config->bearer_token_env != nullptr) {
    const char *token = getenv(config->bearer_token_env);
    if (token == nullptr || token[0] == '\0') {
      ulog_error(
          "Configured webhook bearer-token environment variable is unset");
      return APP_CONFIG_STATUS_SECURITY_ERROR;
    }
    config->bearer_token_owned = http_string_duplicate(token);
    if (config->bearer_token_owned == nullptr) {
      return APP_CONFIG_STATUS_INVALID_VALUE;
    }
    config->bearer_token = config->bearer_token_owned;
  }
  return APP_CONFIG_STATUS_OK;
}
