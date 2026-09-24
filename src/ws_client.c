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

[[nodiscard]] ws_client_t *ws_client_create() {
  auto client = (ws_client_t *)calloc(1, sizeof(ws_client_t));
  if (client == nullptr) {
    return nullptr;
  }

  client->socket_fd = -1;
  client->connected = false;
  client->use_tls = false;
  client->read_timeout_seconds = WS_DEFAULT_READ_TIMEOUT_SECONDS;
  client->write_timeout_seconds = WS_DEFAULT_WRITE_TIMEOUT_SECONDS;
  client->ssl_ctx = nullptr;
  client->ssl = nullptr;
  client->last_error[0] = '\0';
  return client;
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

[[nodiscard]] bool ws_client_set_timeouts(ws_client_t *client,
                                          uint32_t read_timeout_seconds,
                                          uint32_t write_timeout_seconds) {
  if (client == nullptr || read_timeout_seconds == 0 ||
      write_timeout_seconds == 0 || read_timeout_seconds > (uint32_t)INT_MAX ||
      write_timeout_seconds > (uint32_t)INT_MAX) {
    return false;
  }

  client->read_timeout_seconds = read_timeout_seconds;
  client->write_timeout_seconds = write_timeout_seconds;
  return true;
}

[[nodiscard]] bool ws_client_connect(ws_client_t *client, const char *host,
                                     uint16_t port, const char *path) {
  return ws_handshake_connect(client, host, port, path, false);
}

[[nodiscard]] bool ws_client_connect_secure(ws_client_t *client,
                                            const char *host, uint16_t port,
                                            const char *path) {
  return ws_handshake_connect(client, host, port, path, true);
}

[[nodiscard]] bool ws_client_send_text(ws_client_t *client, const char *text,
                                       size_t length) {
  if (client == nullptr || text == nullptr) {
    return false;
  }

  return ws_frame_send(client, 0x1U, (const uint8_t *)text, length);
}

[[nodiscard]] bool ws_client_send_binary(ws_client_t *client,
                                         const uint8_t *data, size_t length) {
  if (client == nullptr || data == nullptr) {
    return false;
  }

  return ws_frame_send(client, 0x2U, data, length);
}

[[nodiscard]] bool ws_client_receive_text(ws_client_t *client, char *buffer,
                                          size_t capacity, size_t *out_length) {
  if (client == nullptr || buffer == nullptr || capacity == 0) {
    return false;
  }

  return ws_frame_receive(client, 0x1U, "text", (uint8_t *)buffer, capacity,
                          out_length, true);
}

[[nodiscard]] bool ws_client_receive_binary(ws_client_t *client,
                                            uint8_t *buffer, size_t capacity,
                                            size_t *out_length) {
  if (client == nullptr || buffer == nullptr || capacity == 0) {
    return false;
  }

  return ws_frame_receive(client, 0x2U, "binary", buffer, capacity, out_length,
                          false);
}

void ws_client_close(ws_client_t *client) {
  if (client == nullptr || !client->connected) {
    return;
  }

  ulog_debug("[ws-client] closing websocket connection fd=%d tls=%s",
             client->socket_fd, client->use_tls ? "true" : "false");
  (void)ws_frame_send(client, 0x8U, nullptr, 0);
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
