#include "websocket-client/http_transmitter_internal.h"
#include "ulog/ulog.h"

#include <brotli/encode.h>
#include <limits.h>
#include <stdlib.h>
#include <zlib.h>
#include <zstd.h>

/*
 * Compression boundary: RabbitMQ payloads are untrusted but validated and
 * bounded before reaching this module. Each returned buffer is owned by the
 * caller and has a size derived from the codec's checked upper-bound API.
 */

[[nodiscard]] const char *
http_transmitter_compression_name(http_compression_t compression) {
  switch (compression) {
  case HTTP_COMPRESSION_NONE:
    return "none";
  case HTTP_COMPRESSION_GZIP:
    return "gzip";
  case HTTP_COMPRESSION_BROTLI:
    return "brotli";
  case HTTP_COMPRESSION_ZSTD:
    return "zstd";
  }
  return "invalid";
}

void http_transmitter_compressed_payload_cleanup(
    http_compressed_payload_t *payload) {
  if (payload == nullptr) {
    return;
  }
  free(payload->data);
  *payload = (http_compressed_payload_t){0};
}

[[nodiscard]] static http_transmitter_status_t
http_compress_gzip(const void *body, size_t body_length,
                   http_compressed_payload_t *payload) {
  if (body_length > UINT_MAX) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  z_stream stream = {0};
  int status = deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                            MAX_WBITS + 16, 8, Z_DEFAULT_STRATEGY);
  if (status != Z_OK) {
    ulog_error("Unable to initialize gzip compressor: %d", status);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  uLong bound = deflateBound(&stream, (uLong)body_length);
  if (bound == 0 || bound > UINT_MAX || bound > SIZE_MAX) {
    (void)deflateEnd(&stream);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  unsigned char *output = calloc((size_t)bound, sizeof(unsigned char));
  if (output == nullptr) {
    (void)deflateEnd(&stream);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  stream.next_in = (Bytef *)body;
  stream.avail_in = (uInt)body_length;
  stream.next_out = output;
  stream.avail_out = (uInt)bound;
  status = deflate(&stream, Z_FINISH);
  int cleanup_status = deflateEnd(&stream);
  if (status != Z_STREAM_END || cleanup_status != Z_OK ||
      stream.total_out > SIZE_MAX) {
    free(output);
    ulog_error("Unable to gzip webhook payload: %d", status);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  *payload = (http_compressed_payload_t){
      .data = output,
      .length = (size_t)stream.total_out,
  };
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] static http_transmitter_status_t
http_compress_brotli(const void *body, size_t body_length,
                     http_compressed_payload_t *payload) {
  size_t output_length = BrotliEncoderMaxCompressedSize(body_length);
  if (output_length == 0) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  unsigned char *output = calloc(output_length, sizeof(unsigned char));
  if (output == nullptr) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  BROTLI_BOOL ok = BrotliEncoderCompress(
      BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_MODE_TEXT,
      body_length, body, &output_length, output);
  if (ok == BROTLI_FALSE) {
    free(output);
    ulog_error("Unable to Brotli-compress webhook payload");
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  *payload = (http_compressed_payload_t){
      .data = output,
      .length = output_length,
  };
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] static http_transmitter_status_t
http_compress_zstd(const void *body, size_t body_length,
                   http_compressed_payload_t *payload) {
  size_t output_capacity = ZSTD_compressBound(body_length);
  if (ZSTD_isError(output_capacity) != 0 || output_capacity == 0) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  unsigned char *output = calloc(output_capacity, sizeof(unsigned char));
  if (output == nullptr) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  size_t output_length = ZSTD_compress(output, output_capacity, body,
                                       body_length, ZSTD_CLEVEL_DEFAULT);
  if (ZSTD_isError(output_length) != 0) {
    ulog_error("Unable to Zstandard-compress webhook payload: %s",
               ZSTD_getErrorName(output_length));
    free(output);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  *payload = (http_compressed_payload_t){
      .data = output,
      .length = output_length,
  };
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] http_transmitter_status_t
http_transmitter_compress_payload(http_compression_t compression,
                                  const void *body, size_t body_length,
                                  http_compressed_payload_t *payload) {
  if (body == nullptr || body_length == 0 ||
      body_length > HTTP_TRANSMITTER_MAX_PAYLOAD_BYTES || payload == nullptr) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  *payload = (http_compressed_payload_t){0};
  switch (compression) {
  case HTTP_COMPRESSION_GZIP:
    return http_compress_gzip(body, body_length, payload);
  case HTTP_COMPRESSION_BROTLI:
    return http_compress_brotli(body, body_length, payload);
  case HTTP_COMPRESSION_ZSTD:
    return http_compress_zstd(body, body_length, payload);
  case HTTP_COMPRESSION_NONE:
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
}
