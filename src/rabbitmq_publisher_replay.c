#include "websocket-client/rabbitmq_publisher_internal.h"

#include "ulog/ulog.h"

#include <inttypes.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

constexpr size_t WS_RABBITMQ_MAX_REPLAY_MESSAGES = 4'096;
constexpr size_t WS_RABBITMQ_MAX_REPLAY_BYTES = 64 * 1'024 * 1'024;
constexpr uint32_t WS_RABBITMQ_RETRY_INITIAL_BACKOFF_MS = 1'000;
constexpr uint32_t WS_RABBITMQ_RETRY_MAX_BACKOFF_MS = 60'000;

/*
 * Queue trust boundary:
 * Payload size and broker availability are externally influenced. Descriptor
 * storage is allocated once, byte growth is bounded, and every payload copy is
 * released only after broker confirmation or publisher destruction.
 */

[[nodiscard]] static uint64_t ws_rabbitmq_now_milliseconds() {
  struct timeval tv = {0};
  if (gettimeofday(&tv, nullptr) == 0) {
    return (uint64_t)tv.tv_sec * 1'000U + (uint64_t)tv.tv_usec / 1'000U;
  }

  time_t now = time(nullptr);
  if (now < 0) {
    return 0;
  }
  return (uint64_t)now * 1'000U;
}

[[nodiscard]] bool
ws_rabbitmq_retry_allows_flush(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  uint64_t now_ms = ws_rabbitmq_now_milliseconds();
  if (publisher->retry_not_before_ms != 0 &&
      now_ms < publisher->retry_not_before_ms) {
    if (publisher->skipped_flush_attempts < UINT32_MAX) {
      publisher->skipped_flush_attempts += 1;
    }
    return false;
  }

  if (publisher->skipped_flush_attempts > 0) {
    ulog_info(
        "RabbitMQ publish retrying after cooldown (queued=%zu skipped=%u)",
        ws_rabbitmq_replay_count(publisher),
        (unsigned)publisher->skipped_flush_attempts);
    publisher->skipped_flush_attempts = 0;
  }

  return true;
}

[[nodiscard]] uint64_t
ws_rabbitmq_retry_delay_ms(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->retry_not_before_ms == 0) {
    return 0;
  }
  uint64_t now_ms = ws_rabbitmq_now_milliseconds();
  return now_ms < publisher->retry_not_before_ms
             ? publisher->retry_not_before_ms - now_ms
             : 0;
}

void ws_rabbitmq_retry_record_failure(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }

  if (publisher->retry_backoff_ms == 0) {
    publisher->retry_backoff_ms = WS_RABBITMQ_RETRY_INITIAL_BACKOFF_MS;
  } else if (publisher->retry_backoff_ms < WS_RABBITMQ_RETRY_MAX_BACKOFF_MS) {
    uint64_t doubled_backoff = (uint64_t)publisher->retry_backoff_ms * 2U;
    publisher->retry_backoff_ms =
        doubled_backoff > WS_RABBITMQ_RETRY_MAX_BACKOFF_MS
            ? WS_RABBITMQ_RETRY_MAX_BACKOFF_MS
            : (uint32_t)doubled_backoff;
  }

  publisher->retry_not_before_ms =
      ws_rabbitmq_now_milliseconds() + publisher->retry_backoff_ms;
  ulog_warn("RabbitMQ unavailable, backing off publish attempts for %u ms "
            "(queued=%zu)",
            (unsigned)publisher->retry_backoff_ms,
            ws_rabbitmq_replay_count(publisher));
}

void ws_rabbitmq_retry_record_success(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }

  if (publisher->retry_backoff_ms > 0) {
    ulog_info("RabbitMQ publish path recovered (queued=%zu)",
              ws_rabbitmq_replay_count(publisher));
  }
  publisher->retry_not_before_ms = 0;
  publisher->retry_backoff_ms = 0;
  publisher->skipped_flush_attempts = 0;
}

[[nodiscard]] bool
ws_rabbitmq_replay_initialize(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }
  publisher->replay_queue =
      calloc(WS_RABBITMQ_MAX_REPLAY_MESSAGES, sizeof(*publisher->replay_queue));
  if (publisher->replay_queue == nullptr) {
    return false;
  }
  publisher->replay_capacity = WS_RABBITMQ_MAX_REPLAY_MESSAGES;
  return true;
}

void ws_rabbitmq_replay_clear(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_queue == nullptr) {
    return;
  }
  for (size_t i = 0; i < publisher->replay_count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    free(publisher->replay_queue[index].payload);
  }
  free(publisher->replay_queue);
  publisher->replay_queue = nullptr;
  publisher->replay_count = 0;
  publisher->replay_bytes = 0;
  publisher->replay_capacity = 0;
  publisher->replay_head = 0;
}

[[nodiscard]] bool
ws_rabbitmq_replay_enqueue(ws_rabbitmq_publisher_t *publisher,
                           const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr ||
      !publisher->mutex_initialized) {
    return false;
  }

  size_t payload_capacity = 0;
  if (ckd_add(&payload_capacity, payload_length, (size_t)1)) {
    return false;
  }
  char *payload_copy = calloc(payload_capacity, sizeof(char));
  if (payload_copy == nullptr) {
    ulog_error("Failed to allocate RabbitMQ payload copy");
    return false;
  }
  if (payload_length > 0) {
    memcpy(payload_copy, payload, payload_length);
  }

  uv_mutex_lock(&publisher->queue_mutex);
  size_t next_replay_bytes = 0;
  bool byte_limit_exceeded =
      ckd_add(&next_replay_bytes, publisher->replay_bytes, payload_length) ||
      next_replay_bytes > WS_RABBITMQ_MAX_REPLAY_BYTES;
  bool queue_full = publisher->stopping ||
                    publisher->replay_count >= publisher->replay_capacity ||
                    byte_limit_exceeded;
  if (queue_full) {
    if (publisher->replay_queue_dropped_messages < UINT64_MAX) {
      publisher->replay_queue_dropped_messages += 1;
    }
    bool should_log = !publisher->replay_queue_full_logged;
    publisher->replay_queue_full_logged = true;
    size_t queued_messages = publisher->replay_count;
    size_t queued_bytes = publisher->replay_bytes;
    uv_mutex_unlock(&publisher->queue_mutex);
    free(payload_copy);
    if (should_log) {
      ulog_error("RabbitMQ outbound queue unavailable (%zu messages, %zu "
                 "bytes; limits=%zu messages/%zu bytes)",
                 queued_messages, queued_bytes, WS_RABBITMQ_MAX_REPLAY_MESSAGES,
                 WS_RABBITMQ_MAX_REPLAY_BYTES);
    }
    return false;
  }

  size_t index = (publisher->replay_head + publisher->replay_count) %
                 publisher->replay_capacity;
  publisher->replay_queue[index] = (ws_rabbitmq_replay_message_t){
      .payload = payload_copy, .payload_length = payload_length};
  publisher->replay_count += 1;
  publisher->replay_bytes = next_replay_bytes;
  uv_mutex_unlock(&publisher->queue_mutex);
  return true;
}

[[nodiscard]] size_t
ws_rabbitmq_replay_peek_batch(ws_rabbitmq_publisher_t *publisher,
                              const ws_rabbitmq_replay_message_t **messages,
                              size_t message_capacity) {
  if (publisher == nullptr || messages == nullptr || message_capacity == 0) {
    return 0;
  }
  uv_mutex_lock(&publisher->queue_mutex);
  size_t count = publisher->replay_count < message_capacity
                     ? publisher->replay_count
                     : message_capacity;
  for (size_t i = 0; i < count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    messages[i] = &publisher->replay_queue[index];
  }
  uv_mutex_unlock(&publisher->queue_mutex);
  return count;
}

void ws_rabbitmq_replay_drop_head(ws_rabbitmq_publisher_t *publisher,
                                  size_t message_count) {
  if (publisher == nullptr || message_count == 0) {
    return;
  }
  uv_mutex_lock(&publisher->queue_mutex);
  size_t drop_count = message_count < publisher->replay_count
                          ? message_count
                          : publisher->replay_count;
  for (size_t i = 0; i < drop_count; ++i) {
    auto message = &publisher->replay_queue[publisher->replay_head];
    size_t payload_length = message->payload_length;
    free(message->payload);
    *message = (ws_rabbitmq_replay_message_t){0};
    publisher->replay_head =
        (publisher->replay_head + 1) % publisher->replay_capacity;
    publisher->replay_count -= 1;
    publisher->replay_bytes = payload_length <= publisher->replay_bytes
                                  ? publisher->replay_bytes - payload_length
                                  : 0;
  }
  bool queue_recovered =
      publisher->replay_queue_full_logged &&
      publisher->replay_count < publisher->replay_capacity / 2;
  uint64_t dropped_messages = publisher->replay_queue_dropped_messages;
  if (queue_recovered) {
    publisher->replay_queue_full_logged = false;
    publisher->replay_queue_dropped_messages = 0;
  }
  uv_mutex_unlock(&publisher->queue_mutex);
  if (queue_recovered) {
    ulog_warn(
        "RabbitMQ outbound queue accepts publishes again (dropped=%" PRIu64 ")",
        dropped_messages);
  }
}

[[nodiscard]] size_t
ws_rabbitmq_replay_count(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || !publisher->mutex_initialized) {
    return 0;
  }
  uv_mutex_lock(&publisher->queue_mutex);
  size_t count = publisher->replay_count;
  uv_mutex_unlock(&publisher->queue_mutex);
  return count;
}
