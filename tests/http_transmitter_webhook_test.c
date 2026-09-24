#include "websocket-client/http_transmitter_internal.h"

#include <assert.h>
#include <brotli/decode.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zlib.h>
#include <zstd.h>

volatile sig_atomic_t http_transmitter_shutdown_signal = 0;

typedef struct test_parallel_delivery_context test_parallel_delivery_context_t;
struct test_parallel_delivery_context {
  http_webhook_client_t *client;
  const char *body;
  size_t body_length;
  http_delivery_result_t result;
};

[[nodiscard]] bool http_transmitter_is_shutdown_requested() {
  return http_transmitter_shutdown_signal != 0;
}

[[nodiscard]] static void *test_parallel_delivery_worker(void *argument) {
  test_parallel_delivery_context_t *context = argument;
  context->result = http_webhook_deliver(context->client, context->body,
                                         context->body_length);
  return nullptr;
}

[[nodiscard]] static uint16_t test_open_server(int *out_socket) {
  int server = socket(AF_INET, SOCK_STREAM, 0);
  assert(server >= 0);
  struct sockaddr_in address = {
      .sin_family = AF_INET,
      .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
      .sin_port = 0,
  };
  assert(bind(server, (const struct sockaddr *)&address, sizeof(address)) == 0);
  assert(listen(server, 4) == 0);
  socklen_t address_length = sizeof(address);
  assert(getsockname(server, (struct sockaddr *)&address, &address_length) ==
         0);
  *out_socket = server;
  return ntohs(address.sin_port);
}

static void test_server_child(int server, uint32_t request_count,
                              const char *status_line,
                              const char *expected_body,
                              bool expect_authorization,
                              const char *expected_encoding) {
  for (uint32_t request = 0; request < request_count; ++request) {
    int client = accept(server, nullptr, nullptr);
    if (client < 0) {
      _exit(EXIT_FAILURE);
    }
    char buffer[8'192] = {0};
    size_t used = 0;
    char *body = nullptr;
    size_t content_length = 0;
    while (used + 1 < sizeof(buffer)) {
      ssize_t received =
          recv(client, buffer + used, sizeof(buffer) - used - 1, 0);
      if (received <= 0) {
        (void)close(client);
        _exit(EXIT_FAILURE);
      }
      used += (size_t)received;
      buffer[used] = '\0';
      if (body == nullptr) {
        char *separator = strstr(buffer, "\r\n\r\n");
        char *length_header = strstr(buffer, "Content-Length: ");
        if (separator != nullptr && length_header != nullptr) {
          body = separator + 4;
          content_length = strtoul(length_header + 16, nullptr, 10);
        }
      }
      if (body != nullptr && used >= (size_t)(body - buffer) + content_length) {
        break;
      }
    }
    bool valid = strstr(buffer, "Content-Type: application/json") != nullptr &&
                 body != nullptr && content_length > 0;
    if (expected_encoding == nullptr) {
      valid = valid && strstr(buffer, "Content-Encoding:") == nullptr &&
              strstr(body, expected_body) != nullptr;
    } else {
      valid = valid && strstr(buffer, expected_encoding) != nullptr;
    }
    if (expect_authorization) {
      valid = valid &&
              strstr(buffer, "Authorization: Bearer test-secret") != nullptr;
    }
    char response[128] = {0};
    int length = snprintf(response, sizeof(response),
                          "HTTP/1.1 %s\r\nContent-Length: 0\r\nConnection: "
                          "close\r\n\r\n",
                          status_line);
    if (!valid || length <= 0 || (size_t)length >= sizeof(response) ||
        send(client, response, (size_t)length, 0) != length) {
      (void)close(client);
      _exit(EXIT_FAILURE);
    }
    (void)close(client);
  }
  (void)close(server);
  _exit(EXIT_SUCCESS);
}

[[nodiscard]] static http_delivery_result_t
test_delivery(uint32_t request_count, const char *status_line,
              uint32_t attempts, bool bearer, http_compression_t compression,
              const char *expected_encoding) {
  constexpr char BODY[] = "{\"hash\":\"0x1234\"}";
  int server = -1;
  uint16_t port = test_open_server(&server);
  pid_t child = fork();
  assert(child >= 0);
  if (child == 0) {
    test_server_child(server, request_count, status_line, BODY, bearer,
                      expected_encoding);
  }

  char url[128] = {0};
  int url_length =
      snprintf(url, sizeof(url), "http://127.0.0.1:%u/webhook", (unsigned)port);
  assert(url_length > 0 && (size_t)url_length < sizeof(url));
  http_transmitter_config_t config = {
      .webhook_url = url,
      .bearer_token = bearer ? "test-secret" : nullptr,
      .connect_timeout = {.value = 1'000},
      .request_timeout = {.value = 2'000},
      .max_attempts = attempts,
      .initial_backoff = {.value = 1},
      .max_backoff = {.value = 2},
      .compression = compression,
  };
  http_webhook_client_t client = {0};
  assert(http_webhook_client_init(&client, &config) ==
         HTTP_TRANSMITTER_STATUS_OK);
  http_delivery_result_t result =
      http_webhook_deliver(&client, BODY, sizeof(BODY) - 1);
  http_webhook_client_cleanup(&client);
  (void)close(server);

  int child_status = 0;
  assert(waitpid(child, &child_status, 0) == child);
  assert(WIFEXITED(child_status));
  assert(WEXITSTATUS(child_status) == EXIT_SUCCESS);
  return result;
}

static void test_parallel_delivery() {
  constexpr size_t WORKER_COUNT = 4;
  constexpr char BODY[] = "{\"hash\":\"0xparallel\"}";
  int server = -1;
  uint16_t port = test_open_server(&server);
  pid_t child = fork();
  assert(child >= 0);
  if (child == 0) {
    test_server_child(server, (uint32_t)WORKER_COUNT, "204 No Content", BODY,
                      false, nullptr);
  }

  char url[128] = {0};
  int url_length =
      snprintf(url, sizeof(url), "http://127.0.0.1:%u/webhook", (unsigned)port);
  assert(url_length > 0 && (size_t)url_length < sizeof(url));
  http_transmitter_config_t config = {
      .webhook_url = url,
      .connect_timeout = {.value = 1'000},
      .request_timeout = {.value = 2'000},
      .max_attempts = 1,
      .initial_backoff = {.value = 1},
      .max_backoff = {.value = 1},
  };
  http_webhook_client_t clients[WORKER_COUNT] = {0};
  pthread_t threads[WORKER_COUNT] = {0};
  test_parallel_delivery_context_t contexts[WORKER_COUNT] = {0};
  for (size_t i = 0; i < WORKER_COUNT; ++i) {
    assert(http_webhook_client_init(&clients[i], &config) ==
           HTTP_TRANSMITTER_STATUS_OK);
  }
  for (size_t i = 0; i < WORKER_COUNT; ++i) {
    contexts[i] = (test_parallel_delivery_context_t){
        .client = &clients[i],
        .body = BODY,
        .body_length = sizeof(BODY) - 1,
    };
    assert(pthread_create(&threads[i], nullptr, test_parallel_delivery_worker,
                          &contexts[i]) == 0);
  }
  for (size_t i = 0; i < WORKER_COUNT; ++i) {
    assert(pthread_join(threads[i], nullptr) == 0);
    assert(contexts[i].result == HTTP_DELIVERY_SUCCESS);
    http_webhook_client_cleanup(&clients[i]);
  }
  (void)close(server);

  int child_status = 0;
  assert(waitpid(child, &child_status, 0) == child);
  assert(WIFEXITED(child_status));
  assert(WEXITSTATUS(child_status) == EXIT_SUCCESS);
}

static void test_compression_round_trip(http_compression_t compression) {
  constexpr char BODY[] =
      "{\"hash\":\"0x1234\",\"repeated\":\"aaaaaaaaaaaaaaaaaaaaaaaa\"}";
  http_compressed_payload_t payload = {0};
  assert(http_transmitter_compress_payload(compression, BODY, sizeof(BODY) - 1,
                                           &payload) ==
         HTTP_TRANSMITTER_STATUS_OK);
  assert(payload.data != nullptr);
  assert(payload.length > 0);

  unsigned char decoded[sizeof(BODY)] = {0};
  size_t decoded_length = sizeof(decoded);
  if (compression == HTTP_COMPRESSION_GZIP) {
    z_stream stream = {
        .next_in = payload.data,
        .avail_in = (uInt)payload.length,
        .next_out = decoded,
        .avail_out = (uInt)decoded_length,
    };
    assert(inflateInit2(&stream, MAX_WBITS + 16) == Z_OK);
    assert(inflate(&stream, Z_FINISH) == Z_STREAM_END);
    decoded_length = (size_t)stream.total_out;
    assert(inflateEnd(&stream) == Z_OK);
  } else if (compression == HTTP_COMPRESSION_BROTLI) {
    assert(BrotliDecoderDecompress(payload.length, payload.data,
                                   &decoded_length,
                                   decoded) == BROTLI_DECODER_RESULT_SUCCESS);
  } else {
    decoded_length =
        ZSTD_decompress(decoded, decoded_length, payload.data, payload.length);
    assert(ZSTD_isError(decoded_length) == 0);
  }
  assert(decoded_length == sizeof(BODY) - 1);
  assert(memcmp(decoded, BODY, decoded_length) == 0);
  http_transmitter_compressed_payload_cleanup(&payload);
}

int main() {
  assert(test_delivery(1, "204 No Content", 1, true, HTTP_COMPRESSION_GZIP,
                       "Content-Encoding: gzip") == HTTP_DELIVERY_SUCCESS);
  assert(test_delivery(1, "204 No Content", 1, false, HTTP_COMPRESSION_BROTLI,
                       "Content-Encoding: br") == HTTP_DELIVERY_SUCCESS);
  assert(test_delivery(1, "204 No Content", 1, false, HTTP_COMPRESSION_ZSTD,
                       "Content-Encoding: zstd") == HTTP_DELIVERY_SUCCESS);
  assert(test_delivery(3, "503 Service Unavailable", 3, false,
                       HTTP_COMPRESSION_NONE,
                       nullptr) == HTTP_DELIVERY_TRANSIENT_FAILURE);
  assert(test_delivery(1, "302 Found", 1, false, HTTP_COMPRESSION_NONE,
                       nullptr) == HTTP_DELIVERY_TRANSIENT_FAILURE);
  test_compression_round_trip(HTTP_COMPRESSION_GZIP);
  test_compression_round_trip(HTTP_COMPRESSION_BROTLI);
  test_compression_round_trip(HTTP_COMPRESSION_ZSTD);
  test_parallel_delivery();

  http_transmitter_config_t config = {
      .webhook_url = "http://127.0.0.1:1/webhook",
      .connect_timeout = {.value = 100},
      .request_timeout = {.value = 100},
      .max_attempts = 1,
      .initial_backoff = {.value = 1},
      .max_backoff = {.value = 1},
  };
  http_webhook_client_t client = {0};
  assert(http_webhook_client_init(&client, &config) ==
         HTTP_TRANSMITTER_STATUS_OK);
  assert(http_webhook_deliver(&client, "not-json", 8) ==
         HTTP_DELIVERY_PERMANENT_FAILURE);
  http_webhook_client_cleanup(&client);
  return EXIT_SUCCESS;
}
