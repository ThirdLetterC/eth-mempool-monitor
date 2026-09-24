#pragma once

#include "app/domain_types.h"
#include "rabbitmq/amqp.h"
#include "ulog/ulog.h"

#include <signal.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Internal console boundary. Configuration owns its *_owned strings; CLI
 * overrides are borrowed argv views and the consumer owns only AMQP handles.
 */
typedef struct rabbitmq_console_config rabbitmq_console_config_t;
struct rabbitmq_console_config {
  const char *rabbitmq_host;
  app_port_t rabbitmq_port;
  const char *rabbitmq_username;
  const char *rabbitmq_password;
  const char *rabbitmq_vhost;
  const char *rabbitmq_queue;
  bool rabbitmq_queue_durable;
  app_rabbitmq_channel_t rabbitmq_channel;
  app_seconds_t rabbitmq_heartbeat;
  app_seconds_t read_timeout;
  app_prefetch_count_t prefetch_count;
  bool auto_ack;
  ulog_level log_level;
  bool log_color;
  char *rabbitmq_host_owned;
  char *rabbitmq_username_owned;
  char *rabbitmq_password_owned;
  char *rabbitmq_vhost_owned;
  char *rabbitmq_queue_owned;
};

typedef struct rabbitmq_console_cli_overrides rabbitmq_console_cli_overrides_t;
struct rabbitmq_console_cli_overrides {
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
  /* Flags make teardown safe after partially completed connection setup. */
  bool logged_in;
  bool channel_open;
};

typedef enum rabbitmq_console_status : uint8_t {
  RABBITMQ_CONSOLE_STATUS_OK = 0,
  RABBITMQ_CONSOLE_STATUS_INVALID_CONFIG,
  RABBITMQ_CONSOLE_STATUS_ALLOCATION_FAILED,
  RABBITMQ_CONSOLE_STATUS_CONNECTION_ERROR,
  RABBITMQ_CONSOLE_STATUS_PROTOCOL_ERROR,
  RABBITMQ_CONSOLE_STATUS_ACK_ERROR,
} rabbitmq_console_status_t;

extern volatile sig_atomic_t app_shutdown_signal;
[[nodiscard]] bool app_is_shutdown_requested();
void app_print_usage(const char *program_name);
void app_config_set_defaults(rabbitmq_console_config_t *config);
void app_config_cleanup(rabbitmq_console_config_t *config);
[[nodiscard]] app_config_status_t
app_parse_cli(int argc, char *argv[],
              rabbitmq_console_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t
app_load_toml_config(rabbitmq_console_config_t *config,
                     const rabbitmq_console_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t
app_apply_cli_overrides(rabbitmq_console_config_t *config,
                        const rabbitmq_console_cli_overrides_t *overrides);
[[nodiscard]] bool app_apply_log_level(ulog_level level);
[[nodiscard]] bool app_apply_log_color(bool enabled);
[[nodiscard]] bool app_apply_log_style_defaults();
[[nodiscard]] rabbitmq_console_status_t
app_rabbitmq_connect(const rabbitmq_console_config_t *config,
                     app_rabbitmq_consumer_t *consumer);
void app_rabbitmq_disconnect(app_rabbitmq_consumer_t *consumer);
[[nodiscard]] rabbitmq_console_status_t
app_consume_loop(app_rabbitmq_consumer_t *consumer,
                 const rabbitmq_console_config_t *config);
void app_handle_payload(const void *body, size_t body_length);
