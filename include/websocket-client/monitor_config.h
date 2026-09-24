#pragma once

#include "app/domain_types.h"
#include "ulog/ulog.h"

#include <stdint.h>

/**
 * Configuration trust boundary:
 * CLI arguments and TOML content are untrusted. The implementation validates
 * types and ranges before configuration is used. Owned strings are released by
 * app_config_cleanup(); callers must not free borrowed string fields.
 */
typedef struct monitor_config monitor_config_t;
struct monitor_config {
  const char *host;
  app_port_t port;
  const char *path;
  const char *request;
  ulog_level log_level;
  bool log_color;
  bool secure;
  const char *redis_host;
  app_port_t redis_port;
  const char *redis_monitored_set_key;
  const char *rabbitmq_host;
  app_port_t rabbitmq_port;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;
  bool rabbitmq_queue_durable;
  app_rabbitmq_channel_t rabbitmq_channel;
  app_seconds_t rabbitmq_heartbeat;
  bool rabbitmq_enabled;
  app_seconds_t read_timeout;
  app_seconds_t write_timeout;
  bool reconnect_enabled;
  app_milliseconds_t reconnect_initial_backoff;
  app_milliseconds_t reconnect_max_backoff;

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

typedef struct monitor_cli_overrides monitor_cli_overrides_t;
struct monitor_cli_overrides {
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
void app_config_set_defaults(monitor_config_t *config);
void app_config_cleanup(monitor_config_t *config);

[[nodiscard]] app_config_status_t
app_parse_cli(int argc, char *argv[], monitor_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t
app_load_toml_config(monitor_config_t *config,
                     const monitor_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t
app_apply_cli_overrides(monitor_config_t *config,
                        const monitor_cli_overrides_t *overrides);

[[nodiscard]] bool app_apply_log_level(ulog_level level);
[[nodiscard]] bool app_apply_log_color(bool enabled);
[[nodiscard]] bool app_apply_log_style_defaults();
