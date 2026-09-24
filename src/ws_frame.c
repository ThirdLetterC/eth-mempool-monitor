#include "websocket-client/ws_client_internal.h"

#include "ulog/ulog.h"

#include <limits.h>
#include <stdckdint.h>
#include <string.h>

constexpr size_t WS_FRAME_SEND_BUFFER_CAPACITY = 4 * 1'024;

/*
 * Frame trust boundary:
 * Frame headers and payload lengths are controlled by the remote peer. Every
 * length is validated before conversion, indexing, or copying. Outbound frames
 * are masked into a bounded stack buffer to avoid per-frame allocation.
 */

[[nodiscard]] ws_status_t ws_frame_send(ws_client_t *client, ws_opcode_t opcode,
                                        const uint8_t *payload,
                                        size_t payload_length) {
  if (client == nullptr || !client->connected) {
    return WS_STATUS_INVALID_STATE;
  }
  if ((payload == nullptr && payload_length != 0) ||
      (opcode != WS_OPCODE_CONTINUATION && opcode != WS_OPCODE_TEXT &&
       opcode != WS_OPCODE_BINARY && opcode != WS_OPCODE_CLOSE &&
       opcode != WS_OPCODE_PING && opcode != WS_OPCODE_PONG)) {
    return WS_STATUS_INVALID_ARGUMENT;
  }
  ulog_trace("[ws-client] send frame opcode=0x%X payload=%zu", (unsigned)opcode,
             payload_length);

  if (payload_length > (size_t)INT64_MAX) {
    ws_set_error(client, "Frame payload is too large");
    return WS_STATUS_INVALID_ARGUMENT;
  }

  uint8_t send_buffer[WS_FRAME_SEND_BUFFER_CAPACITY] = {0};
  size_t offset = 0;
  send_buffer[offset++] = (uint8_t)(0x80U | (opcode & 0x0FU));

  if (payload_length <= 125) {
    send_buffer[offset++] = (uint8_t)(0x80U | (uint8_t)payload_length);
  } else if (payload_length <= UINT16_MAX) {
    send_buffer[offset++] = (uint8_t)(0x80U | 126U);
    send_buffer[offset++] = (uint8_t)((payload_length >> 8U) & 0xFFU);
    send_buffer[offset++] = (uint8_t)(payload_length & 0xFFU);
  } else {
    send_buffer[offset++] = (uint8_t)(0x80U | 127U);
    for (size_t shift = 0; shift < 8; ++shift) {
      auto bits = (7U - shift) * 8U;
      send_buffer[offset++] =
          (uint8_t)(((uint64_t)payload_length >> bits) & 0xFFU);
    }
  }

  uint8_t mask[4] = {0};
  if (!ws_random_bytes(mask, sizeof(mask))) {
    ws_set_error(client, "Failed to generate websocket mask");
    return WS_STATUS_TRANSPORT_ERROR;
  }

  memcpy(send_buffer + offset, mask, sizeof(mask));
  offset += sizeof(mask);

  size_t payload_offset = 0;
  while (true) {
    size_t available = sizeof(send_buffer) - offset;
    size_t remaining = payload_length - payload_offset;
    size_t chunk_length = remaining < available ? remaining : available;

    for (size_t i = 0; i < chunk_length; ++i) {
      uint8_t source =
          payload != nullptr ? payload[payload_offset + i] : (uint8_t)0;
      send_buffer[offset + i] =
          (uint8_t)(source ^ mask[(payload_offset + i) % 4]);
    }

    auto send_status =
        ws_transport_send_all(client->socket_fd, client->ssl, client->use_tls,
                              send_buffer, offset + chunk_length);
    if (send_status != WS_STATUS_OK) {
      ws_set_error(client, "Failed to send frame: %s", strerror(errno));
      return send_status;
    }

    payload_offset += chunk_length;
    if (payload_offset == payload_length) {
      return WS_STATUS_OK;
    }
    offset = 0;
  }
}

[[nodiscard]] ws_status_t ws_frame_receive(ws_client_t *client,
                                           ws_opcode_t expected_opcode,
                                           uint8_t *buffer, size_t capacity,
                                           size_t *out_length,
                                           bool add_nul_terminator) {
  if (client == nullptr || buffer == nullptr || capacity == 0 ||
      (expected_opcode != WS_OPCODE_TEXT &&
       expected_opcode != WS_OPCODE_BINARY)) {
    return WS_STATUS_INVALID_ARGUMENT;
  }
  const char *expected_name =
      expected_opcode == WS_OPCODE_TEXT ? "text" : "binary";
  ulog_trace("[ws-client] receive %s frame(s) capacity=%zu", expected_name,
             capacity);
  size_t total_length = 0;
  bool receiving_fragments = false;
  bool discarding_oversized_message = false;

  while (true) {
    uint8_t header[2] = {0};
    auto read_status = ws_transport_read_exact(
        client, header, sizeof(header), "Reading websocket frame header");
    if (read_status != WS_STATUS_OK) {
      return read_status;
    }

    auto fin = (header[0] & 0x80U) != 0;
    auto opcode = (ws_opcode_t)(header[0] & 0x0FU);
    auto masked = (header[1] & 0x80U) != 0;

    uint64_t payload_length = header[1] & 0x7FU;
    if (payload_length == 126U) {
      uint8_t extended[2] = {0};
      read_status = ws_transport_read_exact(client, extended, sizeof(extended),
                                            "Reading websocket frame length");
      if (read_status != WS_STATUS_OK) {
        return read_status;
      }
      payload_length = ((uint64_t)extended[0] << 8U) | (uint64_t)extended[1];
    } else if (payload_length == 127U) {
      uint8_t extended[8] = {0};
      read_status = ws_transport_read_exact(client, extended, sizeof(extended),
                                            "Reading websocket frame length");
      if (read_status != WS_STATUS_OK) {
        return read_status;
      }
      payload_length = 0;
      for (size_t i = 0; i < 8; ++i) {
        payload_length = (payload_length << 8U) | (uint64_t)extended[i];
      }
    }
    ulog_trace(
        "[ws-client] recv frame fin=%u opcode=0x%X masked=%u payload=%llu",
        fin ? 1U : 0U, (unsigned)opcode, masked ? 1U : 0U,
        (unsigned long long)payload_length);

    uint8_t mask[4] = {0};
    if (masked) {
      read_status = ws_transport_read_exact(client, mask, sizeof(mask),
                                            "Reading websocket frame mask");
      if (read_status != WS_STATUS_OK) {
        return read_status;
      }
    }

    if (opcode == WS_OPCODE_CLOSE) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding close frame payload");
      ws_client_close(client);
      ws_set_error(client, "Connection closed by server");
      return WS_STATUS_PEER_CLOSED;
    }

    if (opcode == WS_OPCODE_PING) {
      if (!fin) {
        ws_set_error(client, "Ping frames must not be fragmented");
        return WS_STATUS_PROTOCOL_ERROR;
      }
      if (payload_length > 125U) {
        ws_set_error(client, "Invalid ping frame length");
        return WS_STATUS_PROTOCOL_ERROR;
      }

      uint8_t ping_payload[125] = {0};
      read_status = ws_transport_read_exact(
          client, ping_payload, (size_t)payload_length, "Reading ping payload");
      if (read_status != WS_STATUS_OK) {
        return read_status;
      }

      if (masked) {
        for (size_t i = 0; i < payload_length; ++i) {
          ping_payload[i] ^= mask[i % 4];
        }
      }

      auto pong_status = ws_frame_send(client, WS_OPCODE_PONG, ping_payload,
                                       (size_t)payload_length);
      if (pong_status != WS_STATUS_OK) {
        return pong_status;
      }
      ulog_trace("[ws-client] ping handled, pong sent payload=%llu",
                 (unsigned long long)payload_length);
      continue;
    }

    if (opcode == WS_OPCODE_PONG) {
      if (!fin) {
        ws_set_error(client, "Pong frames must not be fragmented");
        return WS_STATUS_PROTOCOL_ERROR;
      }
      auto discard_status = ws_transport_discard(client, payload_length,
                                                 "Discarding pong payload");
      if (discard_status != WS_STATUS_OK) {
        return discard_status;
      }
      continue;
    }

    bool is_start = opcode == expected_opcode;
    bool is_continuation = opcode == WS_OPCODE_CONTINUATION;
    if (!is_start && !is_continuation) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding unexpected frame payload");
      ws_set_error(client,
                   "Unexpected websocket opcode 0x%X while waiting for %s data",
                   (unsigned)opcode, expected_name);
      return WS_STATUS_PROTOCOL_ERROR;
    }

    if (is_start) {
      if (receiving_fragments) {
        (void)ws_transport_discard(
            client, payload_length,
            "Discarding unexpected fragmented frame payload");
        ws_set_error(
            client, "Received new %s frame before fragmented message completed",
            expected_name);
        return WS_STATUS_PROTOCOL_ERROR;
      }
      receiving_fragments = true;
    } else if (!receiving_fragments) {
      (void)ws_transport_discard(
          client, payload_length,
          "Discarding unexpected continuation frame payload");
      ws_set_error(client, "Unexpected continuation frame");
      return WS_STATUS_PROTOCOL_ERROR;
    }

    auto terminator_size = add_nul_terminator ? (size_t)1 : (size_t)0;
    size_t required = 0;
    if (payload_length > SIZE_MAX ||
        ckd_add(&required, total_length, (size_t)payload_length) ||
        ckd_add(&required, required, terminator_size)) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding oversized frame payload");
      ws_set_error(client, "Frame too large for this platform");
      return WS_STATUS_PROTOCOL_ERROR;
    }
    if (required > capacity || discarding_oversized_message) {
      auto discard_status = ws_transport_discard(
          client, payload_length, "Discarding oversized frame payload");
      if (discard_status != WS_STATUS_OK) {
        return discard_status;
      }

      total_length += (size_t)payload_length;
      discarding_oversized_message = true;
      if (!fin) {
        continue;
      }

      ws_set_error(client, "Receive buffer is too small (%zu bytes required)",
                   total_length + terminator_size);
      return WS_STATUS_BUFFER_TOO_SMALL;
    }

    read_status = ws_transport_read_exact(client, buffer + total_length,
                                          (size_t)payload_length,
                                          "Reading websocket frame payload");
    if (read_status != WS_STATUS_OK) {
      return read_status;
    }

    if (masked) {
      for (size_t i = 0; i < payload_length; ++i) {
        buffer[total_length + i] ^= mask[i % 4];
      }
    }

    total_length += (size_t)payload_length;

    if (!fin) {
      continue;
    }

    if (add_nul_terminator) {
      buffer[total_length] = '\0';
    }

    if (out_length != nullptr) {
      *out_length = total_length;
    }
    ulog_trace("[ws-client] receive complete type=%s bytes=%zu", expected_name,
               total_length);
    return WS_STATUS_OK;
  }
}
