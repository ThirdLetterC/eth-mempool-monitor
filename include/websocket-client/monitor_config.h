#pragma once

#include "ulog/ulog.h"

#include <stdint.h>

/**
 * Configuration trust boundary:
 * CLI arguments and TOML content are untrusted. The implementation validates
 * types and ranges before configuration is used. Owned strings are released by
 * app_config_cleanup(); callers must not free borrowed string fields.
 */
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

void app_print_usage(const char *program_name);
void app_config_set_defaults(app_config *config);
void app_config_cleanup(app_config *config);

[[nodiscard]] bool app_parse_cli(int argc, char *argv[],
                                 app_cli_overrides *overrides);
[[nodiscard]] bool app_load_toml_config(app_config *config,
                                        const app_cli_overrides *overrides);
[[nodiscard]] bool app_apply_cli_overrides(app_config *config,
                                           const app_cli_overrides *overrides);

[[nodiscard]] bool app_apply_log_level(ulog_level level);
[[nodiscard]] bool app_apply_log_color(bool enabled);
[[nodiscard]] bool app_apply_log_style_defaults();
