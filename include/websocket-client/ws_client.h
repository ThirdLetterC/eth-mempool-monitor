#pragma once

#include "app/domain_types.h"

#include <stddef.h>
#include <stdint.h>

typedef struct ws_client ws_client_t;

typedef enum ws_transport : uint8_t {
  WS_TRANSPORT_PLAIN = 0,
  WS_TRANSPORT_TLS = 1,
} ws_transport_t;

typedef enum ws_status : uint8_t {
  WS_STATUS_OK = 0,
  WS_STATUS_INVALID_ARGUMENT,
  WS_STATUS_INVALID_STATE,
  WS_STATUS_ALLOCATION_FAILED,
  WS_STATUS_TRANSPORT_ERROR,
  WS_STATUS_TLS_ERROR,
  WS_STATUS_PROTOCOL_ERROR,
  WS_STATUS_PEER_CLOSED,
  WS_STATUS_BUFFER_TOO_SMALL,
} ws_status_t;

static_assert(sizeof(ws_transport_t) == sizeof(uint8_t));
static_assert(sizeof(ws_status_t) == sizeof(uint8_t));

typedef struct {
  app_tcp_endpoint_t server;
  const char *path;
  ws_transport_t transport;
} ws_endpoint_t;

typedef struct {
  app_seconds_t read;
  app_seconds_t write;
} ws_timeouts_t;

/**
 * @brief Allocates a websocket client instance.
 * @param out_client receives the owning pointer on success.
 * @return typed allocation or validation status.
 */
[[nodiscard]] ws_status_t ws_client_create(ws_client_t **out_client);

/**
 * @brief Releases all resources owned by the client.
 */
void ws_client_destroy(ws_client_t *client);

/**
 * @brief Connects and performs an RFC6455 handshake using the typed endpoint.
 */
[[nodiscard]] ws_status_t ws_client_connect(ws_client_t *client,
                                            const ws_endpoint_t *endpoint);

/**
 * @brief Sends one text frame.
 */
[[nodiscard]] ws_status_t ws_client_send_text(ws_client_t *client,
                                              const char *text, size_t length);

/**
 * @brief Sends one binary frame.
 */
[[nodiscard]] ws_status_t
ws_client_send_binary(ws_client_t *client, const uint8_t *data, size_t length);

/**
 * @brief Receives one text frame into a caller-owned buffer.
 */
[[nodiscard]] ws_status_t ws_client_receive_text(ws_client_t *client,
                                                 char *buffer, size_t capacity,
                                                 size_t *out_length);

/**
 * @brief Receives one binary frame into a caller-owned buffer.
 */
[[nodiscard]] ws_status_t ws_client_receive_binary(ws_client_t *client,
                                                   uint8_t *buffer,
                                                   size_t capacity,
                                                   size_t *out_length);

/**
 * @brief Sets socket receive/send timeouts in seconds for future connections.
 * @return WS_STATUS_INVALID_ARGUMENT if either timeout is out of range.
 */
[[nodiscard]] ws_status_t ws_client_set_timeouts(ws_client_t *client,
                                                 ws_timeouts_t timeouts);

/**
 * @brief Sends a close frame and closes the socket.
 */
void ws_client_close(ws_client_t *client);

/**
 * @brief Returns the most recent error string.
 */
[[nodiscard]] const char *ws_client_last_error(const ws_client_t *client);

/**
 * @brief Returns a stable diagnostic name for a WebSocket status.
 */
[[nodiscard]] const char *ws_status_string(ws_status_t status);
