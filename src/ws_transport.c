#include "websocket-client/ws_client_internal.h"

#include "ulog/ulog.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <wolfssl/openssl/err.h>
#include <wolfssl/wolfcrypt/ecc.h>

/*
 * Transport trust boundary:
 * DNS results and all bytes received from remote TCP/TLS peers are untrusted.
 * This module owns socket/TLS resources and performs bounded I/O only.
 */

void ws_set_error(ws_client_t *client, const char *format, ...) {
  if (client == nullptr) {
    return;
  }

  va_list args;
  va_start(args, format);
  (void)vsnprintf(client->last_error, sizeof(client->last_error), format, args);
  va_end(args);
  ulog_debug("[ws-client] %s", client->last_error);
}

[[nodiscard]] static bool ws_errno_is_timeout(int err) {
  if (err == EAGAIN || err == EWOULDBLOCK) {
    return true;
  }
#ifdef ETIMEDOUT
  if (err == ETIMEDOUT) {
    return true;
  }
#endif
  return false;
}

void ws_transport_set_read_error(ws_client_t *client, const char *context,
                                 ssize_t transport_result, SSL *ssl,
                                 bool use_tls, uint32_t read_timeout_seconds) {
  if (client == nullptr || context == nullptr) {
    return;
  }

  int saved_errno = errno;
  if (!use_tls || ssl == nullptr) {
    if (transport_result == 0) {
      ws_set_error(client, "%s: connection closed by peer", context);
      return;
    }

    if (ws_errno_is_timeout(saved_errno)) {
      ws_set_error(client, "%s: read timed out after %u seconds", context,
                   (unsigned)read_timeout_seconds);
      return;
    }

    ws_set_error(client, "%s: %s", context, strerror(saved_errno));
    return;
  }

  int ssl_error = SSL_get_error(ssl, (int)transport_result);
  switch (ssl_error) {
  case SSL_ERROR_ZERO_RETURN:
    ws_set_error(client, "%s: TLS connection closed by peer", context);
    return;
  case SSL_ERROR_WANT_READ:
    ws_set_error(client, "%s: TLS read timed out after %u seconds", context,
                 (unsigned)read_timeout_seconds);
    return;
  case SSL_ERROR_WANT_WRITE:
    ws_set_error(client, "%s: TLS write blocked while reading", context);
    return;
  case SSL_ERROR_SYSCALL:
    if (transport_result == 0 || saved_errno == 0) {
      ws_set_error(client, "%s: TLS connection terminated by peer", context);
      return;
    }
    if (ws_errno_is_timeout(saved_errno)) {
      ws_set_error(client, "%s: TLS read timed out after %u seconds", context,
                   (unsigned)read_timeout_seconds);
      return;
    }
    ws_set_error(client, "%s: TLS syscall error: %s", context,
                 strerror(saved_errno));
    return;
  case SSL_ERROR_SSL: {
    unsigned long ssl_lib_error = ERR_get_error();
    if (ssl_lib_error == 0) {
      ws_set_error(client, "%s: TLS protocol error", context);
      return;
    }
    char error_text[128] = {0};
    ERR_error_string_n(ssl_lib_error, error_text, sizeof(error_text));
    ws_set_error(client, "%s: TLS protocol error: %s", context, error_text);
    return;
  }
  default:
    ws_set_error(client, "%s: TLS read failure (SSL_get_error=%d)", context,
                 ssl_error);
    return;
  }
}

[[nodiscard]] ssize_t ws_transport_receive(int fd, SSL *ssl, bool use_tls,
                                           uint8_t *buffer, size_t length) {
  if (use_tls) {
    auto chunk_length = (length > (size_t)INT_MAX) ? INT_MAX : (int)length;
    return SSL_read(ssl, buffer, chunk_length);
  }

  return recv(fd, buffer, length, 0);
}

[[nodiscard]] static ssize_t ws_transport_send(int fd, SSL *ssl, bool use_tls,
                                               const uint8_t *buffer,
                                               size_t length) {
  if (use_tls) {
    auto chunk_length = (length > (size_t)INT_MAX) ? INT_MAX : (int)length;
    auto sent = SSL_write(ssl, buffer, chunk_length);
    return (sent > 0) ? sent : -1;
  }

#ifdef MSG_NOSIGNAL
  return send(fd, buffer, length, MSG_NOSIGNAL);
#else
  return send(fd, buffer, length, 0);
#endif
}

[[nodiscard]] bool ws_transport_read_exact(ws_client_t *client, uint8_t *buffer,
                                           size_t length, const char *context) {
  if (client == nullptr || buffer == nullptr || context == nullptr) {
    return false;
  }

  ulog_trace("[ws-client] read_exact context=\"%s\" bytes=%zu", context,
             length);
  size_t total = 0;
  while (total < length) {
    auto received =
        ws_transport_receive(client->socket_fd, client->ssl, client->use_tls,
                             buffer + total, length - total);
    if (received <= 0) {
      ws_transport_set_read_error(client, context, received, client->ssl,
                                  client->use_tls,
                                  client->read_timeout_seconds);
      return false;
    }
    total += (size_t)received;
  }
  ulog_trace("[ws-client] read_exact complete context=\"%s\" bytes=%zu",
             context, length);
  return true;
}

[[nodiscard]] bool ws_transport_send_all(int fd, SSL *ssl, bool use_tls,
                                         const uint8_t *buffer, size_t length) {
  ulog_trace("[ws-client] send_all tls=%s bytes=%zu",
             use_tls ? "true" : "false", length);
  size_t total = 0;
  while (total < length) {
    auto sent =
        ws_transport_send(fd, ssl, use_tls, buffer + total, length - total);
    if (sent <= 0) {
      return false;
    }
    total += (size_t)sent;
  }
  ulog_trace("[ws-client] send_all complete bytes=%zu", length);
  return true;
}

[[nodiscard]] bool ws_transport_discard(ws_client_t *client, uint64_t length,
                                        const char *context) {
  uint8_t scratch[512] = {0};
  auto remaining = length;

  while (remaining > 0) {
    auto chunk =
        (remaining > sizeof(scratch)) ? sizeof(scratch) : (size_t)remaining;
    if (!ws_transport_read_exact(client, scratch, chunk, context)) {
      return false;
    }
    remaining -= chunk;
  }

  return true;
}

[[nodiscard]] bool ws_random_bytes(uint8_t *buffer, size_t length) {
  if (buffer == nullptr && length != 0) {
    return false;
  }

  auto fd = open("/dev/urandom", O_RDONLY);
  if (fd < 0) {
    return false;
  }

  size_t read_total = 0;
  while (read_total < length) {
    auto bytes = read(fd, buffer + read_total, length - read_total);
    if (bytes > 0) {
      read_total += (size_t)bytes;
      continue;
    }
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    (void)close(fd);
    return false;
  }

  (void)close(fd);
  return true;
}

[[nodiscard]] bool ws_transport_connect_tcp(const char *host, uint16_t port,
                                            uint32_t read_timeout_seconds,
                                            uint32_t write_timeout_seconds,
                                            int *out_fd) {
  ulog_debug("[ws-client] connecting tcp host=%s port=%u read_timeout=%u "
             "write_timeout=%u",
             host, (unsigned)port, (unsigned)read_timeout_seconds,
             (unsigned)write_timeout_seconds);
  char port_text[6] = {0};
  (void)snprintf(port_text, sizeof(port_text), "%u", (unsigned)port);

  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  struct addrinfo *result = nullptr;
  auto status = getaddrinfo(host, port_text, &hints, &result);
  if (status != 0) {
    return false;
  }

  auto connected_fd = -1;
  unsigned attempt = 0;
  for (auto current = result; current != nullptr; current = current->ai_next) {
    attempt += 1;
    auto fd =
        socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd < 0) {
      ulog_trace("[ws-client] socket() attempt=%u failed: %s", attempt,
                 strerror(errno));
      continue;
    }

    struct timeval timeout = {
        .tv_sec = (time_t)read_timeout_seconds,
        .tv_usec = 0,
    };

    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    timeout.tv_sec = (time_t)write_timeout_seconds;
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    if (connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
      connected_fd = fd;
      ulog_debug("[ws-client] tcp connected on attempt=%u fd=%d", attempt, fd);
      break;
    }

    ulog_trace("[ws-client] connect() attempt=%u failed: %s", attempt,
               strerror(errno));
    (void)close(fd);
  }

  freeaddrinfo(result);

  if (connected_fd < 0) {
    ulog_debug("[ws-client] tcp connection failed host=%s port=%u attempts=%u",
               host, (unsigned)port, attempt);
    return false;
  }

  *out_fd = connected_fd;
  return true;
}

[[nodiscard]] bool ws_transport_init_tls_context(ws_client_t *client) {
  if (client->ssl_ctx != nullptr) {
    ulog_trace("[ws-client] reusing existing TLS context");
    return true;
  }

  ulog_trace("[ws-client] creating TLS context");
  auto ctx = SSL_CTX_new(TLS_client_method());
  if (ctx == nullptr) {
    ulog_debug("[ws-client] SSL_CTX_new failed");
    return false;
  }

  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
  if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
    ulog_debug("[ws-client] SSL_CTX_set_default_verify_paths failed");
    SSL_CTX_free(ctx);
    return false;
  }

  client->ssl_ctx = ctx;
  ulog_trace("[ws-client] TLS context created");
  return true;
}

[[nodiscard]] bool ws_transport_connect_tls(const char *host, int socket_fd,
                                            SSL_CTX *ctx, SSL **out_ssl) {
  ulog_debug("[ws-client] starting TLS handshake host=%s fd=%d", host,
             socket_fd);
  auto ssl = SSL_new(ctx);
  if (ssl == nullptr) {
    ulog_debug("[ws-client] SSL_new failed");
    return false;
  }

  if (SSL_set_tlsext_host_name(ssl, host) != 1) {
    ulog_debug("[ws-client] SSL_set_tlsext_host_name failed for host=%s", host);
    SSL_free(ssl);
    return false;
  }

#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  if (SSL_set1_host(ssl, host) != 1) {
    ulog_debug("[ws-client] SSL_set1_host failed for host=%s", host);
    SSL_free(ssl);
    return false;
  }
#endif

  if (SSL_set_fd(ssl, socket_fd) != 1 || SSL_connect(ssl) != 1) {
    ulog_debug("[ws-client] TLS handshake failed host=%s fd=%d", host,
               socket_fd);
    SSL_free(ssl);
    wc_ecc_fp_free();
    return false;
  }

  /* Free retained ECC fixed-point tables after certificate verification. */
  wc_ecc_fp_free();

  *out_ssl = ssl;
  ulog_debug("[ws-client] TLS handshake complete host=%s fd=%d", host,
             socket_fd);
  return true;
}
