#include "websocket-client/ws_client_internal.h"

#include "ulog/ulog.h"

#include <limits.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Frame trust boundary:
 * Frame headers and payload lengths are controlled by the remote peer. Every
 * length is validated before conversion, allocation, indexing, or copying.
 */

[[nodiscard]] bool ws_frame_send(ws_client_t *client, uint8_t opcode,
                                 const uint8_t *payload,
                                 size_t payload_length) {
  if (client == nullptr || !client->connected) {
    return false;
  }
  ulog_trace("[ws-client] send frame opcode=0x%X payload=%zu", (unsigned)opcode,
             payload_length);

  size_t length_field_size = 0;
  if (payload_length <= 125) {
    length_field_size = 0;
  } else if (payload_length <= UINT16_MAX) {
    length_field_size = 2;
  } else {
    length_field_size = 8;
  }

  size_t frame_overhead = 0;
  size_t frame_length = 0;
  if (ckd_add(&frame_overhead, (size_t)6, length_field_size) ||
      ckd_add(&frame_length, frame_overhead, payload_length)) {
    ws_set_error(client, "Frame payload is too large");
    return false;
  }

  auto frame = (uint8_t *)calloc(frame_length, sizeof(uint8_t));
  if (frame == nullptr) {
    ws_set_error(client, "Failed to allocate frame buffer");
    return false;
  }

  size_t offset = 0;
  frame[offset++] = (uint8_t)(0x80U | (opcode & 0x0FU));

  if (payload_length <= 125) {
    frame[offset++] = (uint8_t)(0x80U | (uint8_t)payload_length);
  } else if (payload_length <= UINT16_MAX) {
    frame[offset++] = (uint8_t)(0x80U | 126U);
    frame[offset++] = (uint8_t)((payload_length >> 8U) & 0xFFU);
    frame[offset++] = (uint8_t)(payload_length & 0xFFU);
  } else {
    frame[offset++] = (uint8_t)(0x80U | 127U);
    for (size_t shift = 0; shift < 8; ++shift) {
      auto bits = (7U - shift) * 8U;
      frame[offset++] = (uint8_t)(((uint64_t)payload_length >> bits) & 0xFFU);
    }
  }

  uint8_t mask[4] = {0};
  if (!ws_random_bytes(mask, sizeof(mask))) {
    free(frame);
    ws_set_error(client, "Failed to generate websocket mask");
    return false;
  }

  memcpy(frame + offset, mask, sizeof(mask));
  offset += sizeof(mask);

  for (size_t i = 0; i < payload_length; ++i) {
    auto source = (payload != nullptr) ? payload[i] : 0U;
    frame[offset + i] = source ^ mask[i % 4];
  }

  auto ok = ws_transport_send_all(client->socket_fd, client->ssl,
                                  client->use_tls, frame, frame_length);
  free(frame);

  if (!ok) {
    ws_set_error(client, "Failed to send frame: %s", strerror(errno));
    return false;
  }

  return true;
}

[[nodiscard]] bool ws_frame_receive(ws_client_t *client,
                                    uint8_t expected_opcode,
                                    const char *expected_name, uint8_t *buffer,
                                    size_t capacity, size_t *out_length,
                                    bool add_nul_terminator) {
  ulog_trace("[ws-client] receive %s frame(s) capacity=%zu", expected_name,
             capacity);
  size_t total_length = 0;
  bool receiving_fragments = false;
  bool discarding_oversized_message = false;

  while (true) {
    uint8_t header[2] = {0};
    if (!ws_transport_read_exact(client, header, sizeof(header),
                                 "Reading websocket frame header")) {
      return false;
    }

    auto fin = (header[0] & 0x80U) != 0;
    auto opcode = header[0] & 0x0FU;
    auto masked = (header[1] & 0x80U) != 0;

    uint64_t payload_length = header[1] & 0x7FU;
    if (payload_length == 126U) {
      uint8_t extended[2] = {0};
      if (!ws_transport_read_exact(client, extended, sizeof(extended),
                                   "Reading websocket frame length")) {
        return false;
      }
      payload_length = ((uint64_t)extended[0] << 8U) | (uint64_t)extended[1];
    } else if (payload_length == 127U) {
      uint8_t extended[8] = {0};
      if (!ws_transport_read_exact(client, extended, sizeof(extended),
                                   "Reading websocket frame length")) {
        return false;
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
      if (!ws_transport_read_exact(client, mask, sizeof(mask),
                                   "Reading websocket frame mask")) {
        return false;
      }
    }

    if (opcode == 0x8U) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding close frame payload");
      ws_client_close(client);
      ws_set_error(client, "Connection closed by server");
      return false;
    }

    if (opcode == 0x9U) {
      if (!fin) {
        ws_set_error(client, "Ping frames must not be fragmented");
        return false;
      }
      if (payload_length > 125U) {
        ws_set_error(client, "Invalid ping frame length");
        return false;
      }

      uint8_t ping_payload[125] = {0};
      if (!ws_transport_read_exact(client, ping_payload, (size_t)payload_length,
                                   "Reading ping payload")) {
        return false;
      }

      if (masked) {
        for (size_t i = 0; i < payload_length; ++i) {
          ping_payload[i] ^= mask[i % 4];
        }
      }

      if (!ws_frame_send(client, 0xAU, ping_payload, (size_t)payload_length)) {
        return false;
      }
      ulog_trace("[ws-client] ping handled, pong sent payload=%llu",
                 (unsigned long long)payload_length);
      continue;
    }

    if (opcode == 0xAU) {
      if (!fin) {
        ws_set_error(client, "Pong frames must not be fragmented");
        return false;
      }
      if (!ws_transport_discard(client, payload_length,
                                "Discarding pong payload")) {
        return false;
      }
      continue;
    }

    bool is_start = opcode == expected_opcode;
    bool is_continuation = opcode == 0x0U;
    if (!is_start && !is_continuation) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding unexpected frame payload");
      ws_set_error(client,
                   "Unexpected websocket opcode 0x%X while waiting for %s data",
                   (unsigned)opcode, expected_name);
      return false;
    }

    if (is_start) {
      if (receiving_fragments) {
        (void)ws_transport_discard(
            client, payload_length,
            "Discarding unexpected fragmented frame payload");
        ws_set_error(
            client, "Received new %s frame before fragmented message completed",
            expected_name);
        return false;
      }
      receiving_fragments = true;
    } else if (!receiving_fragments) {
      (void)ws_transport_discard(
          client, payload_length,
          "Discarding unexpected continuation frame payload");
      ws_set_error(client, "Unexpected continuation frame");
      return false;
    }

    auto terminator_size = add_nul_terminator ? (size_t)1 : (size_t)0;
    size_t required = 0;
    if (payload_length > SIZE_MAX ||
        ckd_add(&required, total_length, (size_t)payload_length) ||
        ckd_add(&required, required, terminator_size)) {
      (void)ws_transport_discard(client, payload_length,
                                 "Discarding oversized frame payload");
      ws_set_error(client, "Frame too large for this platform");
      return false;
    }
    if (required > capacity || discarding_oversized_message) {
      if (!ws_transport_discard(client, payload_length,
                                "Discarding oversized frame payload")) {
        return false;
      }

      total_length += (size_t)payload_length;
      discarding_oversized_message = true;
      if (!fin) {
        continue;
      }

      ws_set_error(client, "Receive buffer is too small (%zu bytes required)",
                   total_length + terminator_size);
      return false;
    }

    if (!ws_transport_read_exact(client, buffer + total_length,
                                 (size_t)payload_length,
                                 "Reading websocket frame payload")) {
      return false;
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
    return true;
  }
}
