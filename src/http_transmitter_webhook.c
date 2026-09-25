#include "websocket-client/http_transmitter_internal.h"
#include "parson/parson.h"
#include "ulog/ulog.h"

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(USE_MIMALLOC)
#include <mimalloc.h>
#endif

typedef enum http_payload_validation : uint8_t {
  HTTP_PAYLOAD_VALID = 0,
  HTTP_PAYLOAD_INVALID,
  HTTP_PAYLOAD_VALIDATION_ERROR,
} http_payload_validation_t;

static pthread_once_t http_curl_init_once = PTHREAD_ONCE_INIT;
static CURLcode http_curl_init_status = CURLE_FAILED_INIT;

static void http_curl_global_init_once() {
#if defined(USE_MIMALLOC)
  http_curl_init_status =
      curl_global_init_mem(CURL_GLOBAL_DEFAULT, mi_malloc, mi_free, mi_realloc,
                           mi_strdup, mi_calloc);
#else
  http_curl_init_status = curl_global_init(CURL_GLOBAL_DEFAULT);
#endif
  if (http_curl_init_status == CURLE_OK && atexit(curl_global_cleanup) != 0) {
    curl_global_cleanup();
    http_curl_init_status = CURLE_FAILED_INIT;
  }
}

[[nodiscard]] static size_t http_discard_response(char *data, size_t size,
                                                  size_t count, void *context) {
  (void)data;
  (void)context;
  if (count != 0 && size > SIZE_MAX / count) {
    return 0;
  }
  return size * count;
}

[[nodiscard]] static int http_transfer_progress(void *context,
                                                curl_off_t download_total,
                                                curl_off_t downloaded,
                                                curl_off_t upload_total,
                                                curl_off_t uploaded) {
  (void)context;
  (void)download_total;
  (void)downloaded;
  (void)upload_total;
  (void)uploaded;
  return http_transmitter_is_shutdown_requested() ? 1 : 0;
}

[[nodiscard]] static http_payload_validation_t
http_validate_json_payload(const void *body, size_t body_length) {
  if (body == nullptr || body_length == 0 ||
      body_length > HTTP_TRANSMITTER_MAX_PAYLOAD_BYTES) {
    ulog_error("Rejecting invalid RabbitMQ payload length=%zu", body_length);
    return HTTP_PAYLOAD_INVALID;
  }
  if (memchr(body, '\0', body_length) != nullptr) {
    ulog_error("Rejecting RabbitMQ JSON payload containing an embedded NUL");
    return HTTP_PAYLOAD_INVALID;
  }
  size_t allocation_length = 0;
  if (ckd_add(&allocation_length, body_length, (size_t)1)) {
    ulog_error("Rejecting RabbitMQ JSON payload with overflowing length");
    return HTTP_PAYLOAD_VALIDATION_ERROR;
  }
  char *payload = calloc(allocation_length, sizeof(char));
  if (payload == nullptr) {
    ulog_error("Unable to allocate RabbitMQ JSON validation buffer");
    return HTTP_PAYLOAD_VALIDATION_ERROR;
  }
  memcpy(payload, body, body_length);
  JSON_Value *root = json_parse_string(payload);
  free(payload);
  if (root == nullptr) {
    ulog_error("Rejecting malformed RabbitMQ JSON payload (%zu bytes)",
               body_length);
    return HTTP_PAYLOAD_INVALID;
  }
  bool valid = json_value_get_type(root) == JSONObject;
  json_value_free(root);
  if (!valid) {
    ulog_error("Rejecting RabbitMQ JSON payload that is not an object");
  }
  return valid ? HTTP_PAYLOAD_VALID : HTTP_PAYLOAD_INVALID;
}

[[nodiscard]] static bool http_wait_backoff(uint32_t milliseconds) {
  struct timespec remaining = {
      .tv_sec = (time_t)(milliseconds / 1'000U),
      .tv_nsec = (long)(milliseconds % 1'000U) * 1'000'000L,
  };
  while (!http_transmitter_is_shutdown_requested() &&
         nanosleep(&remaining, &remaining) != 0) {
    if (errno != EINTR) {
      ulog_error("Webhook retry backoff failed: %s", strerror(errno));
      return false;
    }
  }
  return !http_transmitter_is_shutdown_requested();
}

[[nodiscard]] static uint32_t http_next_backoff(uint32_t current,
                                                uint32_t maximum) {
  if (current >= maximum || current > maximum / 2U) {
    return maximum;
  }
  return current * 2U;
}

[[nodiscard]] static bool http_append_header(http_webhook_client_t *client,
                                             const char *header) {
  struct curl_slist *headers = curl_slist_append(client->headers, header);
  if (headers == nullptr) {
    return false;
  }
  client->headers = headers;
  return true;
}

[[nodiscard]] static const char *
http_content_encoding_header(http_compression_t compression) {
  switch (compression) {
  case HTTP_COMPRESSION_NONE:
    return nullptr;
  case HTTP_COMPRESSION_GZIP:
    return "Content-Encoding: gzip";
  case HTTP_COMPRESSION_BROTLI:
    return "Content-Encoding: br";
  case HTTP_COMPRESSION_ZSTD:
    return "Content-Encoding: zstd";
  }
  return nullptr;
}

[[nodiscard]] http_transmitter_status_t
http_webhook_client_init(http_webhook_client_t *client,
                         const http_transmitter_config_t *config) {
  if (client == nullptr || config == nullptr ||
      config->webhook_url == nullptr) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  *client = (http_webhook_client_t){.config = config};
  if (pthread_once(&http_curl_init_once, http_curl_global_init_once) != 0 ||
      http_curl_init_status != CURLE_OK) {
    ulog_error("libcurl global initialization failed: %s",
               curl_easy_strerror(http_curl_init_status));
    return HTTP_TRANSMITTER_STATUS_CURL_ERROR;
  }
  client->easy = curl_easy_init();
  if (client->easy == nullptr) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  if (!http_append_header(client, "Content-Type: application/json")) {
    http_webhook_client_cleanup(client);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  const char *encoding_header =
      http_content_encoding_header(config->compression);
  if (config->compression != HTTP_COMPRESSION_NONE &&
      encoding_header == nullptr) {
    http_webhook_client_cleanup(client);
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  if (encoding_header != nullptr &&
      !http_append_header(client, encoding_header)) {
    http_webhook_client_cleanup(client);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}

void http_webhook_client_cleanup(http_webhook_client_t *client) {
  if (client == nullptr) {
    return;
  }
  if (client->easy != nullptr) {
    curl_easy_cleanup(client->easy);
  }
  if (client->headers != nullptr) {
    curl_slist_free_all(client->headers);
  }
  *client = (http_webhook_client_t){0};
}

[[nodiscard]] static CURLcode
http_webhook_configure_request(http_webhook_client_t *client, const void *body,
                               size_t body_length) {
  if (body_length > (size_t)INT64_MAX) {
    return CURLE_BAD_FUNCTION_ARGUMENT;
  }
  CURL *easy = client->easy;
  const http_transmitter_config_t *config = client->config;
  curl_easy_reset(easy);
#define CURL_SETOPT(OPTION, VALUE)                                             \
  do {                                                                         \
    CURLcode option_status = curl_easy_setopt(easy, (OPTION), (VALUE));        \
    if (option_status != CURLE_OK) {                                           \
      return option_status;                                                    \
    }                                                                          \
  } while (false)
  CURL_SETOPT(CURLOPT_URL, config->webhook_url);
  CURL_SETOPT(CURLOPT_PROTOCOLS_STR, "http,https");
  CURL_SETOPT(CURLOPT_POST, 1L);
  CURL_SETOPT(CURLOPT_POSTFIELDS, body);
  CURL_SETOPT(CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)body_length);
  CURL_SETOPT(CURLOPT_HTTPHEADER, client->headers);
  CURL_SETOPT(CURLOPT_CONNECTTIMEOUT_MS, (long)config->connect_timeout.value);
  CURL_SETOPT(CURLOPT_TIMEOUT_MS, (long)config->request_timeout.value);
  CURL_SETOPT(CURLOPT_FOLLOWLOCATION, 0L);
  CURL_SETOPT(CURLOPT_SSL_VERIFYPEER, 1L);
  CURL_SETOPT(CURLOPT_SSL_VERIFYHOST, 2L);
  CURL_SETOPT(CURLOPT_WRITEFUNCTION, http_discard_response);
  CURL_SETOPT(CURLOPT_XFERINFOFUNCTION, http_transfer_progress);
  CURL_SETOPT(CURLOPT_NOPROGRESS, 0L);
  CURL_SETOPT(CURLOPT_NOSIGNAL, 1L);
  CURL_SETOPT(CURLOPT_USERAGENT, "eth-mempool-monitor/http-transmitter");
  if (config->bearer_token != nullptr) {
    CURL_SETOPT(CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
    CURL_SETOPT(CURLOPT_XOAUTH2_BEARER, config->bearer_token);
  }
#undef CURL_SETOPT
  return CURLE_OK;
}

[[nodiscard]] static http_delivery_result_t
http_webhook_deliver_prepared(http_webhook_client_t *client, const void *body,
                              size_t body_length) {
  uint32_t backoff = client->config->initial_backoff.value;
  for (uint32_t attempt = 1; attempt <= client->config->max_attempts;
       ++attempt) {
    if (http_transmitter_is_shutdown_requested()) {
      return HTTP_DELIVERY_SHUTDOWN;
    }
    CURLcode status = http_webhook_configure_request(client, body, body_length);
    if (status == CURLE_OK) {
      status = curl_easy_perform(client->easy);
    }
    long response_code = 0;
    if (status == CURLE_OK) {
      status = curl_easy_getinfo(client->easy, CURLINFO_RESPONSE_CODE,
                                 &response_code);
    }
    if (status == CURLE_OK && response_code >= 200 && response_code < 300) {
      ulog_debug("Webhook accepted RabbitMQ event with HTTP %ld",
                 response_code);
      return HTTP_DELIVERY_SUCCESS;
    }
    if (http_transmitter_is_shutdown_requested()) {
      return HTTP_DELIVERY_SHUTDOWN;
    }
    if (status != CURLE_OK) {
      ulog_warn("Webhook attempt %u/%u failed: %s", attempt,
                client->config->max_attempts, curl_easy_strerror(status));
    } else {
      ulog_warn("Webhook attempt %u/%u returned HTTP %ld", attempt,
                client->config->max_attempts, response_code);
    }
    if (attempt < client->config->max_attempts) {
      if (!http_wait_backoff(backoff)) {
        return HTTP_DELIVERY_SHUTDOWN;
      }
      backoff = http_next_backoff(backoff, client->config->max_backoff.value);
    }
  }
  return HTTP_DELIVERY_TRANSIENT_FAILURE;
}

[[nodiscard]] http_delivery_result_t
http_webhook_deliver(http_webhook_client_t *client, const void *body,
                     size_t body_length) {
  if (client == nullptr || client->easy == nullptr ||
      client->config == nullptr) {
    return HTTP_DELIVERY_TRANSIENT_FAILURE;
  }
  http_payload_validation_t validation =
      http_validate_json_payload(body, body_length);
  if (validation == HTTP_PAYLOAD_INVALID) {
    return HTTP_DELIVERY_PERMANENT_FAILURE;
  }
  if (validation == HTTP_PAYLOAD_VALIDATION_ERROR) {
    return HTTP_DELIVERY_TRANSIENT_FAILURE;
  }

  const void *request_body = body;
  size_t request_length = body_length;
  http_compressed_payload_t compressed = {0};
  if (client->config->compression != HTTP_COMPRESSION_NONE) {
    http_transmitter_status_t status = http_transmitter_compress_payload(
        client->config->compression, body, body_length, &compressed);
    if (status != HTTP_TRANSMITTER_STATUS_OK) {
      return HTTP_DELIVERY_TRANSIENT_FAILURE;
    }
    request_body = compressed.data;
    request_length = compressed.length;
  }

  http_delivery_result_t result =
      http_webhook_deliver_prepared(client, request_body, request_length);
  http_transmitter_compressed_payload_cleanup(&compressed);
  return result;
}
