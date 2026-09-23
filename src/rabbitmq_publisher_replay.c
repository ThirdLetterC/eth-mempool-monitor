#include "rabbitmq_publisher_internal.h"

#include "ulog/ulog.h"

#include <inttypes.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

constexpr size_t WS_RABBITMQ_MAX_REPLAY_MESSAGES = 4'096;
constexpr uint32_t WS_RABBITMQ_RETRY_INITIAL_BACKOFF_MS = 1'000;
constexpr uint32_t WS_RABBITMQ_RETRY_MAX_BACKOFF_MS = 60'000;

/*
 * Replay-queue trust boundary:
 * Payload size and broker availability are externally influenced. Queue growth
 * is bounded, allocation sizes are checked, and retry backoff is capped.
 */

[[nodiscard]] static uint64_t ws_rabbitmq_now_milliseconds() {
  struct timeval tv = {0};
  if (gettimeofday(&tv, nullptr) == 0) {
    return (uint64_t)tv.tv_sec * 1000U + (uint64_t)tv.tv_usec / 1000U;
  }

  time_t now = time(nullptr);
  if (now < 0) {
    return 0;
  }
  return (uint64_t)now * 1000U;
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
        publisher->replay_count, (unsigned)publisher->skipped_flush_attempts);
    publisher->skipped_flush_attempts = 0;
  }

  return true;
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
            (unsigned)publisher->retry_backoff_ms, publisher->replay_count);
}

void ws_rabbitmq_retry_record_success(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }

  if (publisher->retry_backoff_ms > 0) {
    ulog_info("RabbitMQ publish path recovered (queued=%zu)",
              publisher->replay_count);
  }
  publisher->retry_not_before_ms = 0;
  publisher->retry_backoff_ms = 0;
  publisher->skipped_flush_attempts = 0;
}

void ws_rabbitmq_replay_clear(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_queue == nullptr) {
    return;
  }
  if (publisher->replay_capacity == 0) {
    free(publisher->replay_queue);
    publisher->replay_queue = nullptr;
    publisher->replay_count = 0;
    publisher->replay_head = 0;
    return;
  }
  for (size_t i = 0; i < publisher->replay_count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    free(publisher->replay_queue[index].payload);
  }
  free(publisher->replay_queue);
  publisher->replay_queue = nullptr;
  publisher->replay_count = 0;
  publisher->replay_capacity = 0;
  publisher->replay_head = 0;
}

[[nodiscard]] static bool
ws_rabbitmq_reserve_replay_capacity(ws_rabbitmq_publisher_t *publisher,
                                    size_t new_capacity) {
  if (publisher == nullptr) {
    return false;
  }
  if (new_capacity <= publisher->replay_capacity) {
    return true;
  }
  if (new_capacity > SIZE_MAX / sizeof(*publisher->replay_queue)) {
    return false;
  }

  auto resized = (ws_rabbitmq_replay_message_t *)calloc(
      new_capacity, sizeof(*publisher->replay_queue));
  if (resized == nullptr) {
    return false;
  }

  for (size_t i = 0; i < publisher->replay_count; ++i) {
    size_t index = (publisher->replay_head + i) % publisher->replay_capacity;
    resized[i] = publisher->replay_queue[index];
  }
  free(publisher->replay_queue);

  publisher->replay_queue = resized;
  publisher->replay_capacity = new_capacity;
  publisher->replay_head = 0;
  return true;
}

[[nodiscard]] static ws_rabbitmq_replay_message_t *
ws_rabbitmq_replay_head_message(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_count == 0 ||
      publisher->replay_queue == nullptr ||
      publisher->replay_head >= publisher->replay_capacity) {
    return nullptr;
  }
  return &publisher->replay_queue[publisher->replay_head];
}

[[nodiscard]] bool
ws_rabbitmq_replay_enqueue(ws_rabbitmq_publisher_t *publisher,
                           const char *payload, size_t payload_length) {
  if (publisher == nullptr || payload == nullptr) {
    return false;
  }

  if (publisher->replay_count >= WS_RABBITMQ_MAX_REPLAY_MESSAGES) {
    if (publisher->replay_queue_dropped_messages < UINT64_MAX) {
      publisher->replay_queue_dropped_messages += 1;
    }
    if (!publisher->replay_queue_full_logged) {
      ulog_error(
          "RabbitMQ replay queue is full (%zu messages), dropping publishes "
          "until broker recovers",
          publisher->replay_count);
      publisher->replay_queue_full_logged = true;
    }
    return false;
  }

  size_t payload_capacity = 0;
  if (ckd_add(&payload_capacity, payload_length, (size_t)1)) {
    return false;
  }

  char *payload_copy = calloc(payload_capacity, sizeof(char));
  if (payload_copy == nullptr) {
    ulog_error("Failed to allocate replay payload copy");
    return false;
  }
  if (payload_length > 0) {
    memcpy(payload_copy, payload, payload_length);
  }

  if (publisher->replay_count == publisher->replay_capacity) {
    size_t new_capacity =
        publisher->replay_capacity == 0 ? 32 : publisher->replay_capacity * 2;
    if (new_capacity > WS_RABBITMQ_MAX_REPLAY_MESSAGES) {
      new_capacity = WS_RABBITMQ_MAX_REPLAY_MESSAGES;
    }
    if (new_capacity < publisher->replay_count) {
      free(payload_copy);
      return false;
    }

    if (!ws_rabbitmq_reserve_replay_capacity(publisher, new_capacity)) {
      free(payload_copy);
      return false;
    }
  }

  if (publisher->replay_queue_full_logged) {
    ulog_warn("RabbitMQ replay queue accepts publishes again (dropped=%" PRIu64
              ")",
              publisher->replay_queue_dropped_messages);
    publisher->replay_queue_full_logged = false;
    publisher->replay_queue_dropped_messages = 0;
  }

  size_t index = (publisher->replay_head + publisher->replay_count) %
                 publisher->replay_capacity;
  publisher->replay_queue[index] = (ws_rabbitmq_replay_message_t){
      .payload = payload_copy, .payload_length = payload_length};
  publisher->replay_count += 1;
  return true;
}

static void ws_rabbitmq_drop_replay_head(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr || publisher->replay_count == 0 ||
      publisher->replay_queue == nullptr) {
    return;
  }

  free(publisher->replay_queue[publisher->replay_head].payload);
  publisher->replay_queue[publisher->replay_head] =
      (ws_rabbitmq_replay_message_t){0};
  publisher->replay_head =
      (publisher->replay_head + 1) % publisher->replay_capacity;
  publisher->replay_count -= 1;

  if (publisher->replay_count == 0) {
    free(publisher->replay_queue);
    publisher->replay_queue = nullptr;
    publisher->replay_capacity = 0;
    publisher->replay_head = 0;
  }
}

[[nodiscard]] bool
ws_rabbitmq_replay_flush(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  while (publisher->replay_count > 0) {
    ws_rabbitmq_replay_message_t *message =
        ws_rabbitmq_replay_head_message(publisher);
    if (message == nullptr) {
      return false;
    }
    if (!ws_rabbitmq_connection_publish(publisher, message->payload,
                                        message->payload_length)) {
      return false;
    }

    ws_rabbitmq_drop_replay_head(publisher);
  }

  return true;
}
