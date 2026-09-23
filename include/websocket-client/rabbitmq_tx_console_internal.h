#pragma once

#include "rabbitmq/amqp.h"
#include "ulog/ulog.h"

#include <signal.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Internal console boundary. Configuration owns its *_owned strings; CLI
 * overrides are borrowed argv views and the consumer owns only AMQP handles.
 */
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
  /* Flags make teardown safe after partially completed connection setup. */
  bool logged_in;
  bool channel_open;
};

extern volatile sig_atomic_t app_shutdown_signal;
[[nodiscard]] bool app_is_shutdown_requested();
void app_print_usage(const char *program_name);
void app_config_set_defaults(app_config_t *config);
void app_config_cleanup(app_config_t *config);
[[nodiscard]] bool app_parse_cli(int argc, char *argv[],
                                 app_cli_overrides_t *overrides);
[[nodiscard]] bool app_load_toml_config(app_config_t *config,
                                        const app_cli_overrides_t *overrides);
[[nodiscard]] bool
app_apply_cli_overrides(app_config_t *config,
                        const app_cli_overrides_t *overrides);
[[nodiscard]] bool app_apply_log_level(ulog_level level);
[[nodiscard]] bool app_apply_log_color(bool enabled);
[[nodiscard]] bool app_apply_log_style_defaults();
[[nodiscard]] bool app_rabbitmq_connect(const app_config_t *config,
                                        app_rabbitmq_consumer_t *consumer);
void app_rabbitmq_disconnect(app_rabbitmq_consumer_t *consumer);
[[nodiscard]] bool app_consume_loop(app_rabbitmq_consumer_t *consumer,
                                    const app_config_t *config);
void app_handle_payload(const void *body, size_t body_length);
