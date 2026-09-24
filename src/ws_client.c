#include "websocket-client/ws_client_internal.h"

#include "ulog/ulog.h"

#include <limits.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

constexpr uint32_t WS_DEFAULT_READ_TIMEOUT_SECONDS = 5;
constexpr uint32_t WS_DEFAULT_WRITE_TIMEOUT_SECONDS = 5;

/*
 * Client lifecycle trust boundary:
 * Public arguments are caller-controlled. This module validates handles and
 * delegates untrusted network processing to the handshake and frame modules.
 */

[[nodiscard]] ws_status_t ws_client_create(ws_client_t **out_client) {
  if (out_client == nullptr) {
    return WS_STATUS_INVALID_ARGUMENT;
  }
  *out_client = nullptr;

  auto client = (ws_client_t *)calloc(1, sizeof(ws_client_t));
  if (client == nullptr) {
    return WS_STATUS_ALLOCATION_FAILED;
  }

  client->socket_fd = -1;
  client->connected = false;
  client->use_tls = false;
  client->read_timeout_seconds = WS_DEFAULT_READ_TIMEOUT_SECONDS;
  client->write_timeout_seconds = WS_DEFAULT_WRITE_TIMEOUT_SECONDS;
  client->ssl_ctx = nullptr;
  client->ssl = nullptr;
  client->last_error[0] = '\0';
  *out_client = client;
  return WS_STATUS_OK;
}

void ws_client_destroy(ws_client_t *client) {
  if (client == nullptr) {
    return;
  }

  ws_client_close(client);
  if (client->ssl_ctx != nullptr) {
    SSL_CTX_free(client->ssl_ctx);
    client->ssl_ctx = nullptr;
  }
  free(client);
}

[[nodiscard]] ws_status_t ws_client_set_timeouts(ws_client_t *client,
                                                 ws_timeouts_t timeouts) {
  if (client == nullptr || timeouts.read.value == 0 ||
      timeouts.write.value == 0 || timeouts.read.value > (uint32_t)INT_MAX ||
      timeouts.write.value > (uint32_t)INT_MAX) {
    return WS_STATUS_INVALID_ARGUMENT;
  }

  client->read_timeout_seconds = timeouts.read.value;
  client->write_timeout_seconds = timeouts.write.value;
  return WS_STATUS_OK;
}

[[nodiscard]] ws_status_t ws_client_connect(ws_client_t *client,
                                            const ws_endpoint_t *endpoint) {
  return ws_handshake_connect(client, endpoint);
}

[[nodiscard]] ws_status_t ws_client_send_text(ws_client_t *client,
                                              const char *text, size_t length) {
  if (client == nullptr || text == nullptr) {
    return WS_STATUS_INVALID_ARGUMENT;
  }

  return ws_frame_send(client, WS_OPCODE_TEXT, (const uint8_t *)text, length);
}

[[nodiscard]] ws_status_t
ws_client_send_binary(ws_client_t *client, const uint8_t *data, size_t length) {
  if (client == nullptr || data == nullptr) {
    return WS_STATUS_INVALID_ARGUMENT;
  }

  return ws_frame_send(client, WS_OPCODE_BINARY, data, length);
}

[[nodiscard]] ws_status_t ws_client_receive_text(ws_client_t *client,
                                                 char *buffer, size_t capacity,
                                                 size_t *out_length) {
  if (client == nullptr || buffer == nullptr || capacity == 0) {
    return WS_STATUS_INVALID_ARGUMENT;
  }

  return ws_frame_receive(client, WS_OPCODE_TEXT, (uint8_t *)buffer, capacity,
                          out_length, true);
}

[[nodiscard]] ws_status_t ws_client_receive_binary(ws_client_t *client,
                                                   uint8_t *buffer,
                                                   size_t capacity,
                                                   size_t *out_length) {
  if (client == nullptr || buffer == nullptr || capacity == 0) {
    return WS_STATUS_INVALID_ARGUMENT;
  }

  return ws_frame_receive(client, WS_OPCODE_BINARY, buffer, capacity,
                          out_length, false);
}

void ws_client_close(ws_client_t *client) {
  if (client == nullptr || !client->connected) {
    return;
  }

  ulog_debug("[ws-client] closing websocket connection fd=%d tls=%s",
             client->socket_fd, client->use_tls ? "true" : "false");
  (void)ws_frame_send(client, WS_OPCODE_CLOSE, nullptr, 0);
  if (client->ssl != nullptr) {
    (void)SSL_shutdown(client->ssl);
    SSL_free(client->ssl);
    client->ssl = nullptr;
  }
  (void)shutdown(client->socket_fd, SHUT_RDWR);
  (void)close(client->socket_fd);
  client->socket_fd = -1;
  client->connected = false;
  client->use_tls = false;
}

[[nodiscard]] const char *ws_client_last_error(const ws_client_t *client) {
  if (client == nullptr) {
    return "ws_client_t pointer is nullptr";
  }

  if (client->last_error[0] == '\0') {
    return "no error";
  }

  return client->last_error;
}

[[nodiscard]] const char *ws_status_string(ws_status_t status) {
  switch (status) {
  case WS_STATUS_OK:
    return "ok";
  case WS_STATUS_INVALID_ARGUMENT:
    return "invalid argument";
  case WS_STATUS_INVALID_STATE:
    return "invalid state";
  case WS_STATUS_ALLOCATION_FAILED:
    return "allocation failed";
  case WS_STATUS_TRANSPORT_ERROR:
    return "transport error";
  case WS_STATUS_TLS_ERROR:
    return "TLS error";
  case WS_STATUS_PROTOCOL_ERROR:
    return "protocol error";
  case WS_STATUS_PEER_CLOSED:
    return "peer closed";
  case WS_STATUS_BUFFER_TOO_SMALL:
    return "buffer too small";
  }
}
