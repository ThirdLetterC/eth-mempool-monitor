#pragma once

#include "websocket-client/ws_client.h"

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <wolfssl/options.h>
#include <wolfssl/openssl/ssl.h>

/*
 * Private WebSocket transport boundary. ws_client owns socket_fd, ssl_ctx,
 * and ssl while connected; public callers interact with the opaque type only.
 */
constexpr size_t WS_ERROR_MESSAGE_CAPACITY = 256;

struct ws_client {
  int socket_fd;
  bool connected;
  bool use_tls;
  uint32_t read_timeout_seconds;
  uint32_t write_timeout_seconds;
  SSL_CTX *ssl_ctx;
  SSL *ssl;
  char last_error[WS_ERROR_MESSAGE_CAPACITY];
};

/* Error helpers always write within the fixed last_error buffer. */
void ws_set_error(ws_client_t *client, const char *format, ...);
void ws_transport_set_read_error(ws_client_t *client, const char *context,
                                 ssize_t transport_result, SSL *ssl,
                                 bool use_tls, uint32_t read_timeout_seconds);

/* Transport helpers borrow buffers and never retain caller-owned pointers. */
[[nodiscard]] ssize_t ws_transport_receive(int fd, SSL *ssl, bool use_tls,
                                           uint8_t *buffer, size_t length);
[[nodiscard]] bool ws_transport_read_exact(ws_client_t *client, uint8_t *buffer,
                                           size_t length, const char *context);
[[nodiscard]] bool ws_transport_send_all(int fd, SSL *ssl, bool use_tls,
                                         const uint8_t *buffer, size_t length);
[[nodiscard]] bool ws_transport_discard(ws_client_t *client, uint64_t length,
                                        const char *context);
[[nodiscard]] bool ws_random_bytes(uint8_t *buffer, size_t length);
[[nodiscard]] bool ws_transport_connect_tcp(const char *host, uint16_t port,
                                            uint32_t read_timeout_seconds,
                                            uint32_t write_timeout_seconds,
                                            int *out_fd);
[[nodiscard]] bool ws_transport_init_tls_context(ws_client_t *client);
[[nodiscard]] bool ws_transport_connect_tls(const char *host, int socket_fd,
                                            SSL_CTX *ctx, SSL **out_ssl);

[[nodiscard]] bool ws_handshake_connect(ws_client_t *client, const char *host,
                                        uint16_t port, const char *path,
                                        bool use_tls);

[[nodiscard]] bool ws_frame_send(ws_client_t *client, uint8_t opcode,
                                 const uint8_t *payload, size_t payload_length);
[[nodiscard]] bool ws_frame_receive(ws_client_t *client,
                                    uint8_t expected_opcode,
                                    const char *expected_name, uint8_t *buffer,
                                    size_t capacity, size_t *out_length,
                                    bool add_nul_terminator);
