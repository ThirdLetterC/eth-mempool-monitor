#pragma once

#include <stdint.h>

/*
 * Strong scalar types used at first-party configuration and API boundaries.
 * The wrappers intentionally require explicit extraction before passing values
 * to operating-system or third-party APIs.
 */
typedef struct {
  uint16_t value;
} app_port_t;

typedef struct {
  uint32_t value;
} app_seconds_t;

typedef struct {
  uint32_t value;
} app_milliseconds_t;

typedef struct {
  int32_t value;
} app_socket_backlog_t;

typedef struct {
  uint16_t value;
} app_rabbitmq_channel_t;

typedef struct {
  uint16_t value;
} app_prefetch_count_t;

typedef struct {
  const char *host;
  app_port_t port;
} app_tcp_endpoint_t;

typedef enum app_config_status : uint8_t {
  APP_CONFIG_STATUS_OK = 0,
  APP_CONFIG_STATUS_INVALID_ARGUMENT,
  APP_CONFIG_STATUS_LOAD_ERROR,
  APP_CONFIG_STATUS_INVALID_VALUE,
  APP_CONFIG_STATUS_LOG_ERROR,
  APP_CONFIG_STATUS_SECURITY_ERROR,
} app_config_status_t;

[[nodiscard]] static inline bool app_port_from_u64(uint64_t value,
                                                   app_port_t *out_port) {
  if (out_port == nullptr || value == 0 || value > UINT16_MAX) {
    return false;
  }
  out_port->value = (uint16_t)value;
  return true;
}

[[nodiscard]] static inline bool
app_seconds_from_u64(uint64_t value, app_seconds_t *out_seconds) {
  if (out_seconds == nullptr || value == 0 || value > UINT32_MAX) {
    return false;
  }
  out_seconds->value = (uint32_t)value;
  return true;
}

[[nodiscard]] static inline bool
app_milliseconds_from_u64(uint64_t value,
                          app_milliseconds_t *out_milliseconds) {
  if (out_milliseconds == nullptr || value == 0 || value > UINT32_MAX) {
    return false;
  }
  out_milliseconds->value = (uint32_t)value;
  return true;
}

[[nodiscard]] static inline bool
app_socket_backlog_from_u64(uint64_t value, app_socket_backlog_t *out_backlog) {
  if (out_backlog == nullptr || value == 0 || value > INT32_MAX) {
    return false;
  }
  out_backlog->value = (int32_t)value;
  return true;
}

[[nodiscard]] static inline bool
app_rabbitmq_channel_from_u64(uint64_t value,
                              app_rabbitmq_channel_t *out_channel) {
  if (out_channel == nullptr || value == 0 || value > UINT16_MAX) {
    return false;
  }
  out_channel->value = (uint16_t)value;
  return true;
}

[[nodiscard]] static inline bool
app_prefetch_count_from_u64(uint64_t value,
                            app_prefetch_count_t *out_prefetch_count) {
  if (out_prefetch_count == nullptr || value > UINT16_MAX) {
    return false;
  }
  out_prefetch_count->value = (uint16_t)value;
  return true;
}

static_assert(sizeof(app_port_t) == sizeof(uint16_t));
static_assert(sizeof(app_seconds_t) == sizeof(uint32_t));
static_assert(sizeof(app_milliseconds_t) == sizeof(uint32_t));
static_assert(sizeof(app_socket_backlog_t) == sizeof(int32_t));
static_assert(sizeof(app_rabbitmq_channel_t) == sizeof(uint16_t));
static_assert(sizeof(app_prefetch_count_t) == sizeof(uint16_t));
static_assert(sizeof(app_config_status_t) == sizeof(uint8_t));
