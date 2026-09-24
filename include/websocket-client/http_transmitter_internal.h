#pragma once

#include "app/domain_types.h"
#include "rabbitmq/amqp.h"
#include "ulog/ulog.h"

#include <curl/curl.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>

static constexpr size_t HTTP_TRANSMITTER_MAX_PAYLOAD_BYTES = 1 * 1'024 * 1'024;

typedef enum http_compression : uint8_t {
  HTTP_COMPRESSION_NONE = 0,
  HTTP_COMPRESSION_GZIP,
  HTTP_COMPRESSION_BROTLI,
  HTTP_COMPRESSION_ZSTD,
} http_compression_t;

typedef struct http_compressed_payload http_compressed_payload_t;
struct http_compressed_payload {
  unsigned char *data;
  size_t length;
};

/*
 * Internal transmitter boundary.
 *
 * RabbitMQ payloads and HTTP responses cross untrusted network boundaries.
 * Configuration owns every *_owned string. The consumer thread alone owns
 * AMQP handles and settlement, each bounded work slot owns its payload copy,
 * and every webhook worker owns one libcurl handle and header list.
 */
typedef struct http_transmitter_config http_transmitter_config_t;
struct http_transmitter_config {
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
  const char *webhook_url;
  const char *bearer_token_env;
  const char *bearer_token;
  app_milliseconds_t connect_timeout;
  app_milliseconds_t request_timeout;
  uint32_t max_attempts;
  app_milliseconds_t initial_backoff;
  app_milliseconds_t max_backoff;
  app_parallel_request_count_t parallel_requests;
  http_compression_t compression;
  ulog_level log_level;
  bool log_color;
  char *rabbitmq_host_owned;
  char *rabbitmq_username_owned;
  char *rabbitmq_password_owned;
  char *rabbitmq_vhost_owned;
  char *rabbitmq_queue_owned;
  char *webhook_url_owned;
  char *bearer_token_env_owned;
  char *bearer_token_owned;
};

typedef struct http_transmitter_cli_overrides http_transmitter_cli_overrides_t;
struct http_transmitter_cli_overrides {
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
  const char *webhook_url;
  const char *bearer_token_env;
  const char *connect_timeout_ms_text;
  const char *request_timeout_ms_text;
  const char *max_attempts_text;
  const char *initial_backoff_ms_text;
  const char *max_backoff_ms_text;
  const char *parallel_requests_text;
  const char *compression;
  bool show_help;
};

typedef struct http_transmitter_consumer http_transmitter_consumer_t;
struct http_transmitter_consumer {
  amqp_connection_state_t connection;
  amqp_channel_t channel;
  bool logged_in;
  bool channel_open;
  uint32_t pending_messages_at_connect;
};

typedef struct http_webhook_client http_webhook_client_t;
struct http_webhook_client {
  CURL *easy;
  struct curl_slist *headers;
  const http_transmitter_config_t *config;
};

typedef enum http_transmitter_status : uint8_t {
  HTTP_TRANSMITTER_STATUS_OK = 0,
  HTTP_TRANSMITTER_STATUS_INVALID_CONFIG,
  HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED,
  HTTP_TRANSMITTER_STATUS_CONNECTION_ERROR,
  HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR,
  HTTP_TRANSMITTER_STATUS_ACK_ERROR,
  HTTP_TRANSMITTER_STATUS_CURL_ERROR,
  HTTP_TRANSMITTER_STATUS_THREAD_ERROR,
} http_transmitter_status_t;

typedef enum http_delivery_result : uint8_t {
  HTTP_DELIVERY_SUCCESS = 0,
  HTTP_DELIVERY_PERMANENT_FAILURE,
  HTTP_DELIVERY_TRANSIENT_FAILURE,
  HTTP_DELIVERY_SHUTDOWN,
} http_delivery_result_t;

static_assert(sizeof(http_transmitter_status_t) == sizeof(uint8_t));
static_assert(sizeof(http_delivery_result_t) == sizeof(uint8_t));
static_assert(sizeof(http_compression_t) == sizeof(uint8_t));

extern volatile sig_atomic_t http_transmitter_shutdown_signal;

[[nodiscard]] bool http_transmitter_is_shutdown_requested();
void http_transmitter_print_usage(const char *program_name);
void http_transmitter_config_set_defaults(http_transmitter_config_t *config);
void http_transmitter_config_cleanup(http_transmitter_config_t *config);
[[nodiscard]] app_config_status_t
http_transmitter_parse_cli(int argc, char *argv[],
                           http_transmitter_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t http_transmitter_load_toml_config(
    http_transmitter_config_t *config,
    const http_transmitter_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t http_transmitter_apply_cli_overrides(
    http_transmitter_config_t *config,
    const http_transmitter_cli_overrides_t *overrides);
[[nodiscard]] app_config_status_t
http_transmitter_finalize_config(http_transmitter_config_t *config);

[[nodiscard]] bool http_transmitter_apply_log_level(ulog_level level);
[[nodiscard]] bool http_transmitter_apply_log_color(bool enabled);
[[nodiscard]] bool http_transmitter_apply_log_style_defaults();
[[nodiscard]] const char *
http_transmitter_compression_name(http_compression_t compression);
[[nodiscard]] http_transmitter_status_t
http_transmitter_compress_payload(http_compression_t compression,
                                  const void *body, size_t body_length,
                                  http_compressed_payload_t *payload);
void http_transmitter_compressed_payload_cleanup(
    http_compressed_payload_t *payload);

[[nodiscard]] http_transmitter_status_t
http_transmitter_rabbitmq_connect(const http_transmitter_config_t *config,
                                  http_transmitter_consumer_t *consumer);
void http_transmitter_rabbitmq_disconnect(
    http_transmitter_consumer_t *consumer);
[[nodiscard]] http_transmitter_status_t
http_transmitter_consume_loop(http_transmitter_consumer_t *consumer,
                              const http_transmitter_config_t *config);

[[nodiscard]] http_transmitter_status_t
http_webhook_client_init(http_webhook_client_t *client,
                         const http_transmitter_config_t *config);
void http_webhook_client_cleanup(http_webhook_client_t *client);
[[nodiscard]] http_delivery_result_t
http_webhook_deliver(http_webhook_client_t *client, const void *body,
                     size_t body_length);
