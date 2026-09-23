#define _POSIX_C_SOURCE 200809L

#include "websocket-client/ws_client_internal.h"

#include "ulog/ulog.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

constexpr size_t WS_HTTP_RESPONSE_CAPACITY = 8 * 1024;
constexpr size_t WS_MAX_HEADER_VALUE = 256;
constexpr uint16_t WS_HANDSHAKE_VERSION = 13;

constexpr char WS_ACCEPT_GUID[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
constexpr char WS_BASE64_TABLE[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

typedef struct {
  uint32_t state[5];
  uint64_t total_bits;
  uint8_t block[64];
  size_t block_length;
} sha1_ctx_t;

/*
 * Handshake trust boundary:
 * Host, path, and HTTP upgrade bytes originate outside the process. Responses
 * are accepted only after bounded parsing and RFC 6455 accept-key validation.
 */

[[nodiscard]] static uint32_t ws_rotl32(uint32_t value, unsigned shift) {
  return (value << shift) | (value >> (32U - shift));
}

static void ws_sha1_init(sha1_ctx_t *ctx) {
  ctx->state[0] = 0x67452301U;
  ctx->state[1] = 0xEFCDAB89U;
  ctx->state[2] = 0x98BADCFEU;
  ctx->state[3] = 0x10325476U;
  ctx->state[4] = 0xC3D2E1F0U;
  ctx->total_bits = 0;
  ctx->block_length = 0;
}

static void ws_sha1_transform(sha1_ctx_t *ctx, const uint8_t *block) {
  uint32_t w[80] = {0};
  for (size_t i = 0; i < 16; ++i) {
    auto offset = i * 4;
    w[i] = ((uint32_t)block[offset] << 24U) |
           ((uint32_t)block[offset + 1] << 16U) |
           ((uint32_t)block[offset + 2] << 8U) | (uint32_t)block[offset + 3];
  }

  for (size_t i = 16; i < 80; ++i) {
    w[i] = ws_rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1U);
  }

  auto a = ctx->state[0];
  auto b = ctx->state[1];
  auto c = ctx->state[2];
  auto d = ctx->state[3];
  auto e = ctx->state[4];

  for (size_t i = 0; i < 80; ++i) {
    uint32_t f = 0;
    uint32_t k = 0;

    if (i < 20) {
      f = (b & c) | ((~b) & d);
      k = 0x5A827999U;
    } else if (i < 40) {
      f = b ^ c ^ d;
      k = 0x6ED9EBA1U;
    } else if (i < 60) {
      f = (b & c) | (b & d) | (c & d);
      k = 0x8F1BBCDCU;
    } else {
      f = b ^ c ^ d;
      k = 0xCA62C1D6U;
    }

    auto temp = ws_rotl32(a, 5U) + f + e + k + w[i];
    e = d;
    d = c;
    c = ws_rotl32(b, 30U);
    b = a;
    a = temp;
  }

  ctx->state[0] += a;
  ctx->state[1] += b;
  ctx->state[2] += c;
  ctx->state[3] += d;
  ctx->state[4] += e;
}

static void ws_sha1_update(sha1_ctx_t *ctx, const uint8_t *data,
                           size_t length) {
  if (length == 0) {
    return;
  }

  ctx->total_bits += (uint64_t)length * 8U;

  size_t index = 0;
  while (index < length) {
    auto space = sizeof(ctx->block) - ctx->block_length;
    auto remaining = length - index;
    auto to_copy = (remaining < space) ? remaining : space;

    memcpy(ctx->block + ctx->block_length, data + index, to_copy);
    ctx->block_length += to_copy;
    index += to_copy;

    if (ctx->block_length == sizeof(ctx->block)) {
      ws_sha1_transform(ctx, ctx->block);
      ctx->block_length = 0;
    }
  }
}

static void ws_sha1_final(sha1_ctx_t *ctx, uint8_t digest[20]) {
  ctx->block[ctx->block_length++] = 0x80U;

  if (ctx->block_length > 56) {
    while (ctx->block_length < 64) {
      ctx->block[ctx->block_length++] = 0;
    }
    ws_sha1_transform(ctx, ctx->block);
    ctx->block_length = 0;
  }

  while (ctx->block_length < 56) {
    ctx->block[ctx->block_length++] = 0;
  }

  for (size_t i = 0; i < 8; ++i) {
    auto shift = (7U - i) * 8U;
    ctx->block[56 + i] = (uint8_t)((ctx->total_bits >> shift) & 0xFFU);
  }

  ws_sha1_transform(ctx, ctx->block);

  for (size_t i = 0; i < 5; ++i) {
    digest[i * 4] = (uint8_t)((ctx->state[i] >> 24U) & 0xFFU);
    digest[i * 4 + 1] = (uint8_t)((ctx->state[i] >> 16U) & 0xFFU);
    digest[i * 4 + 2] = (uint8_t)((ctx->state[i] >> 8U) & 0xFFU);
    digest[i * 4 + 3] = (uint8_t)(ctx->state[i] & 0xFFU);
  }
}

[[nodiscard]] static size_t ws_base64_encode(const uint8_t *input,
                                             size_t input_length, char *output,
                                             size_t output_capacity) {
  auto required = ((input_length + 2U) / 3U) * 4U + 1U;
  if (output_capacity < required) {
    return 0;
  }

  size_t src = 0;
  size_t dst = 0;

  while (src + 2U < input_length) {
    auto chunk = ((uint32_t)input[src] << 16U) |
                 ((uint32_t)input[src + 1] << 8U) | (uint32_t)input[src + 2];

    output[dst++] = WS_BASE64_TABLE[(chunk >> 18U) & 0x3FU];
    output[dst++] = WS_BASE64_TABLE[(chunk >> 12U) & 0x3FU];
    output[dst++] = WS_BASE64_TABLE[(chunk >> 6U) & 0x3FU];
    output[dst++] = WS_BASE64_TABLE[chunk & 0x3FU];
    src += 3;
  }

  if (src < input_length) {
    auto chunk = (uint32_t)input[src] << 16U;
    if (src + 1U < input_length) {
      chunk |= (uint32_t)input[src + 1] << 8U;
    }

    output[dst++] = WS_BASE64_TABLE[(chunk >> 18U) & 0x3FU];
    output[dst++] = WS_BASE64_TABLE[(chunk >> 12U) & 0x3FU];

    if (src + 1U < input_length) {
      output[dst++] = WS_BASE64_TABLE[(chunk >> 6U) & 0x3FU];
    } else {
      output[dst++] = '=';
    }

    output[dst++] = '=';
  }

  output[dst] = '\0';
  return dst;
}

[[nodiscard]] static bool ws_find_header_value(const char *response,
                                               const char *header_name,
                                               char *value,
                                               size_t value_capacity) {
  auto header_name_length = strlen(header_name);
  auto line = strstr(response, "\r\n");
  if (line == nullptr) {
    return false;
  }
  line += 2;

  while (*line != '\0') {
    auto line_end = strstr(line, "\r\n");
    if (line_end == nullptr) {
      return false;
    }
    if (line_end == line) {
      return false;
    }

    auto line_length = (size_t)(line_end - line);
    const char *colon = (const char *)memchr(line, ':', line_length);
    if (colon != nullptr) {
      auto name_length = (size_t)(colon - line);
      if (name_length == header_name_length &&
          strncasecmp(line, header_name, header_name_length) == 0) {
        const char *raw = colon + 1;
        while (*raw == ' ' || *raw == '\t') {
          ++raw;
        }

        const char *raw_end = line_end;
        while (raw_end > raw && (raw_end[-1] == ' ' || raw_end[-1] == '\t')) {
          --raw_end;
        }

        auto value_length = (size_t)(raw_end - raw);
        if (value_length + 1 > value_capacity) {
          return false;
        }

        memcpy(value, raw, value_length);
        value[value_length] = '\0';
        return true;
      }
    }

    line = line_end + 2;
  }

  return false;
}

[[nodiscard]] static bool ws_header_has_token(const char *value,
                                              const char *token) {
  auto token_length = strlen(token);
  auto cursor = value;

  while (*cursor != '\0') {
    while (*cursor == ' ' || *cursor == '\t' || *cursor == ',') {
      ++cursor;
    }

    auto start = cursor;
    while (*cursor != '\0' && *cursor != ',') {
      ++cursor;
    }

    auto end = cursor;
    while (end > start && (end[-1] == ' ' || end[-1] == '\t')) {
      --end;
    }

    if ((size_t)(end - start) == token_length &&
        strncasecmp(start, token, token_length) == 0) {
      return true;
    }

    if (*cursor == ',') {
      ++cursor;
    }
  }

  return false;
}

[[nodiscard]] static bool ws_compute_accept_value(const char *key, char *output,
                                                  size_t output_capacity) {
  char challenge[128] = {0};
  auto printed =
      snprintf(challenge, sizeof(challenge), "%s%s", key, WS_ACCEPT_GUID);
  if (printed < 0 || (size_t)printed >= sizeof(challenge)) {
    return false;
  }

  sha1_ctx_t sha1 = {0};
  uint8_t digest[20] = {0};

  ws_sha1_init(&sha1);
  ws_sha1_update(&sha1, (const uint8_t *)challenge, (size_t)printed);
  ws_sha1_final(&sha1, digest);

  return ws_base64_encode(digest, sizeof(digest), output, output_capacity) != 0;
}

[[nodiscard]] bool ws_handshake_connect(ws_client_t *client, const char *host,
                                        uint16_t port, const char *path,
                                        bool use_tls) {
  if (client == nullptr || host == nullptr || path == nullptr) {
    return false;
  }

  ulog_debug("[ws-client] connect start scheme=%s host=%s port=%u path=%s",
             use_tls ? "wss" : "ws", host, (unsigned)port, path);
  if (client->connected) {
    ulog_trace("[ws-client] closing previous connection before reconnect");
    ws_client_close(client);
  }

  int socket_fd = -1;
  if (!ws_transport_connect_tcp(host, port, client->read_timeout_seconds,
                                client->write_timeout_seconds, &socket_fd)) {
    ws_set_error(client, "Failed to connect to %s:%u", host, (unsigned)port);
    return false;
  }

  SSL *ssl = nullptr;
  if (use_tls) {
    if (!ws_transport_init_tls_context(client) ||
        !ws_transport_connect_tls(host, socket_fd, client->ssl_ctx, &ssl)) {
      ws_set_error(client, "Failed to establish TLS with %s:%u", host,
                   (unsigned)port);
      (void)close(socket_fd);
      return false;
    }
  }

  uint8_t key_raw[16] = {0};
  if (!ws_random_bytes(key_raw, sizeof(key_raw))) {
    ws_set_error(client, "Failed to generate handshake key");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char key_encoded[32] = {0};
  if (ws_base64_encode(key_raw, sizeof(key_raw), key_encoded,
                       sizeof(key_encoded)) == 0) {
    ws_set_error(client, "Failed to encode handshake key");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char request[1024] = {0};
  auto request_length = snprintf(request, sizeof(request),
                                 "GET %s HTTP/1.1\r\n"
                                 "Host: %s:%u\r\n"
                                 "Upgrade: websocket\r\n"
                                 "Connection: Upgrade\r\n"
                                 "Sec-WebSocket-Key: %s\r\n"
                                 "Sec-WebSocket-Version: %u\r\n"
                                 "\r\n",
                                 path, host, (unsigned)port, key_encoded,
                                 (unsigned)WS_HANDSHAKE_VERSION);

  if (request_length < 0 || (size_t)request_length >= sizeof(request)) {
    ws_set_error(client, "Handshake request is too large");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  if (!ws_transport_send_all(socket_fd, ssl, use_tls, (const uint8_t *)request,
                             (size_t)request_length)) {
    ws_set_error(client, "Failed to send handshake: %s", strerror(errno));
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }
  ulog_trace("[ws-client] websocket handshake request sent bytes=%zu",
             (size_t)request_length);

  char response[WS_HTTP_RESPONSE_CAPACITY] = {0};
  size_t response_length = 0;
  while (response_length + 1 < sizeof(response)) {
    uint8_t byte = 0;
    auto bytes = ws_transport_receive(socket_fd, ssl, use_tls, &byte, 1);
    if (bytes <= 0) {
      ws_transport_set_read_error(client, "Receiving handshake response", bytes,
                                  ssl, use_tls, client->read_timeout_seconds);
      if (ssl != nullptr) {
        SSL_free(ssl);
      }
      (void)close(socket_fd);
      return false;
    }

    response[response_length++] = (char)byte;
    response[response_length] = '\0';

    if (response_length >= 4 &&
        memcmp(response + response_length - 4, "\r\n\r\n", 4) == 0) {
      break;
    }
  }

  if (strstr(response, "\r\n\r\n") == nullptr) {
    ws_set_error(client, "Incomplete handshake response");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  if (strncmp(response, "HTTP/1.1 101", 12) != 0 &&
      strncmp(response, "HTTP/1.0 101", 12) != 0) {
    ws_set_error(client, "Server rejected websocket upgrade");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char upgrade_header[WS_MAX_HEADER_VALUE] = {0};
  if (!ws_find_header_value(response, "Upgrade", upgrade_header,
                            sizeof(upgrade_header)) ||
      strcasecmp(upgrade_header, "websocket") != 0) {
    ws_set_error(client, "Handshake missing valid Upgrade header");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char connection_header[WS_MAX_HEADER_VALUE] = {0};
  if (!ws_find_header_value(response, "Connection", connection_header,
                            sizeof(connection_header)) ||
      !ws_header_has_token(connection_header, "Upgrade")) {
    ws_set_error(client, "Handshake missing valid Connection header");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char accept_received[WS_MAX_HEADER_VALUE] = {0};
  if (!ws_find_header_value(response, "Sec-WebSocket-Accept", accept_received,
                            sizeof(accept_received))) {
    ws_set_error(client, "Handshake missing Sec-WebSocket-Accept");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  char accept_expected[WS_MAX_HEADER_VALUE] = {0};
  if (!ws_compute_accept_value(key_encoded, accept_expected,
                               sizeof(accept_expected))) {
    ws_set_error(client, "Failed to compute expected accept key");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  if (strcmp(accept_received, accept_expected) != 0) {
    ws_set_error(client, "Invalid Sec-WebSocket-Accept header");
    if (ssl != nullptr) {
      SSL_free(ssl);
    }
    (void)close(socket_fd);
    return false;
  }

  client->socket_fd = socket_fd;
  client->connected = true;
  client->use_tls = use_tls;
  client->ssl = ssl;
  client->last_error[0] = '\0';
  ulog_debug("[ws-client] websocket connection established scheme=%s host=%s "
             "port=%u path=%s fd=%d",
             use_tls ? "wss" : "ws", host, (unsigned)port, path, socket_fd);
  return true;
}
