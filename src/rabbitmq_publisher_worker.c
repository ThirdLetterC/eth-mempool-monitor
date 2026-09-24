#include "websocket-client/rabbitmq_publisher_internal.h"

#include "ulog/ulog.h"

constexpr size_t WS_RABBITMQ_PUBLISH_BATCH_SIZE = 128;
constexpr size_t WS_RABBITMQ_BATCHES_PER_TURN = 32;

static void ws_rabbitmq_worker_timer_cb(uv_timer_t *handle);

/*
 * Concurrency boundary:
 * Caller threads may enqueue owned payload copies. Only this event-loop thread
 * touches rabbitmq-c state, so blocking broker calls cannot stall ingestion and
 * no AMQP object requires cross-thread synchronization.
 */

[[nodiscard]] static bool
ws_rabbitmq_worker_is_stopping(ws_rabbitmq_publisher_t *publisher) {
  uv_mutex_lock(&publisher->queue_mutex);
  bool stopping = publisher->stopping;
  uv_mutex_unlock(&publisher->queue_mutex);
  return stopping;
}

static void
ws_rabbitmq_worker_close_handles(ws_rabbitmq_publisher_t *publisher) {
  if (publisher->retry_timer_initialized &&
      !uv_is_closing((const uv_handle_t *)&publisher->retry_timer)) {
    (void)uv_timer_stop(&publisher->retry_timer);
    uv_close((uv_handle_t *)&publisher->retry_timer, nullptr);
  }
  if (publisher->work_async_initialized &&
      !uv_is_closing((const uv_handle_t *)&publisher->work_async)) {
    uv_close((uv_handle_t *)&publisher->work_async, nullptr);
  }
}

static void
ws_rabbitmq_worker_schedule_retry(ws_rabbitmq_publisher_t *publisher) {
  uint64_t delay_ms = ws_rabbitmq_retry_delay_ms(publisher);
  if (delay_ms == 0) {
    delay_ms = 1;
  }
  int status = uv_timer_start(&publisher->retry_timer,
                              ws_rabbitmq_worker_timer_cb, delay_ms, 0);
  if (status != 0) {
    ulog_error("Failed to schedule RabbitMQ retry: %s", uv_strerror(status));
    ws_rabbitmq_worker_close_handles(publisher);
  }
}

static void ws_rabbitmq_worker_process(ws_rabbitmq_publisher_t *publisher) {
  bool stopping = ws_rabbitmq_worker_is_stopping(publisher);
  if (!stopping && !ws_rabbitmq_retry_allows_flush(publisher)) {
    ws_rabbitmq_worker_schedule_retry(publisher);
    return;
  }

  for (size_t batch_number = 0; batch_number < WS_RABBITMQ_BATCHES_PER_TURN;
       ++batch_number) {
    const ws_rabbitmq_replay_message_t
        *messages[WS_RABBITMQ_PUBLISH_BATCH_SIZE] = {nullptr};
    size_t message_count = ws_rabbitmq_replay_peek_batch(
        publisher, messages, WS_RABBITMQ_PUBLISH_BATCH_SIZE);
    if (message_count == 0) {
      ws_rabbitmq_retry_record_success(publisher);
      (void)uv_timer_stop(&publisher->retry_timer);
      if (ws_rabbitmq_worker_is_stopping(publisher)) {
        ws_rabbitmq_worker_close_handles(publisher);
      }
      return;
    }

    if (!ws_rabbitmq_connection_publish_batch(publisher, messages,
                                              message_count)) {
      ws_rabbitmq_retry_record_failure(publisher);
      if (ws_rabbitmq_worker_is_stopping(publisher)) {
        ulog_error("RabbitMQ shutdown flush failed with %zu queued message%s",
                   ws_rabbitmq_replay_count(publisher),
                   ws_rabbitmq_replay_count(publisher) == 1 ? "" : "s");
        ws_rabbitmq_worker_close_handles(publisher);
      } else {
        ws_rabbitmq_worker_schedule_retry(publisher);
      }
      return;
    }

    ws_rabbitmq_replay_drop_head(publisher, message_count);
    ws_rabbitmq_retry_record_success(publisher);
  }

  if (ws_rabbitmq_replay_count(publisher) > 0) {
    int status = uv_async_send(&publisher->work_async);
    if (status != 0) {
      ulog_error("Failed to continue RabbitMQ queue drain: %s",
                 uv_strerror(status));
    }
  } else if (ws_rabbitmq_worker_is_stopping(publisher)) {
    ws_rabbitmq_worker_close_handles(publisher);
  }
}

static void ws_rabbitmq_worker_async_cb(uv_async_t *handle) {
  auto publisher = (ws_rabbitmq_publisher_t *)handle->data;
  if (publisher != nullptr) {
    ws_rabbitmq_worker_process(publisher);
  }
}

static void ws_rabbitmq_worker_timer_cb(uv_timer_t *handle) {
  auto publisher = (ws_rabbitmq_publisher_t *)handle->data;
  if (publisher != nullptr) {
    ws_rabbitmq_worker_process(publisher);
  }
}

static void ws_rabbitmq_worker_main(void *context) {
  auto publisher = (ws_rabbitmq_publisher_t *)context;
  bool connection_open = ws_rabbitmq_connection_open(publisher);

  uv_mutex_lock(&publisher->queue_mutex);
  publisher->worker_start_ok = connection_open;
  publisher->worker_ready = true;
  uv_cond_signal(&publisher->startup_condition);
  uv_mutex_unlock(&publisher->queue_mutex);

  if (!connection_open) {
    ws_rabbitmq_worker_close_handles(publisher);
  }
  (void)uv_run(&publisher->worker_loop, UV_RUN_DEFAULT);
  ws_rabbitmq_connection_close(publisher);

  int close_status = uv_loop_close(&publisher->worker_loop);
  if (close_status != 0) {
    ulog_error("Failed to close RabbitMQ worker loop: %s",
               uv_strerror(close_status));
  }
}

[[nodiscard]] bool
ws_rabbitmq_worker_start(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return false;
  }

  int status = uv_mutex_init(&publisher->queue_mutex);
  if (status != 0) {
    ulog_error("Failed to initialize RabbitMQ queue mutex: %s",
               uv_strerror(status));
    return false;
  }
  publisher->mutex_initialized = true;

  status = uv_cond_init(&publisher->startup_condition);
  if (status != 0) {
    ulog_error("Failed to initialize RabbitMQ startup condition: %s",
               uv_strerror(status));
    return false;
  }
  publisher->condition_initialized = true;

  if (!ws_rabbitmq_replay_initialize(publisher)) {
    ulog_error("Failed to allocate bounded RabbitMQ outbound queue");
    return false;
  }

  status = uv_loop_init(&publisher->worker_loop);
  if (status != 0) {
    ulog_error("Failed to initialize RabbitMQ worker loop: %s",
               uv_strerror(status));
    return false;
  }
  publisher->loop_initialized = true;

  status = uv_async_init(&publisher->worker_loop, &publisher->work_async,
                         ws_rabbitmq_worker_async_cb);
  if (status != 0) {
    ulog_error("Failed to initialize RabbitMQ work signal: %s",
               uv_strerror(status));
    (void)uv_loop_close(&publisher->worker_loop);
    publisher->loop_initialized = false;
    return false;
  }
  publisher->work_async.data = publisher;
  publisher->work_async_initialized = true;

  status = uv_timer_init(&publisher->worker_loop, &publisher->retry_timer);
  if (status != 0) {
    ulog_error("Failed to initialize RabbitMQ retry timer: %s",
               uv_strerror(status));
    ws_rabbitmq_worker_close_handles(publisher);
    (void)uv_run(&publisher->worker_loop, UV_RUN_DEFAULT);
    (void)uv_loop_close(&publisher->worker_loop);
    publisher->loop_initialized = false;
    return false;
  }
  publisher->retry_timer.data = publisher;
  publisher->retry_timer_initialized = true;

  status = uv_thread_create(&publisher->worker_thread, ws_rabbitmq_worker_main,
                            publisher);
  if (status != 0) {
    ulog_error("Failed to start RabbitMQ worker thread: %s",
               uv_strerror(status));
    ws_rabbitmq_worker_close_handles(publisher);
    (void)uv_run(&publisher->worker_loop, UV_RUN_DEFAULT);
    (void)uv_loop_close(&publisher->worker_loop);
    publisher->loop_initialized = false;
    return false;
  }
  publisher->worker_started = true;

  uv_mutex_lock(&publisher->queue_mutex);
  while (!publisher->worker_ready) {
    uv_cond_wait(&publisher->startup_condition, &publisher->queue_mutex);
  }
  bool started = publisher->worker_start_ok;
  uv_mutex_unlock(&publisher->queue_mutex);

  if (!started) {
    (void)uv_thread_join(&publisher->worker_thread);
    publisher->worker_started = false;
    publisher->loop_initialized = false;
    publisher->work_async_initialized = false;
    publisher->retry_timer_initialized = false;
  }
  return started;
}

void ws_rabbitmq_worker_stop(ws_rabbitmq_publisher_t *publisher) {
  if (publisher == nullptr) {
    return;
  }
  if (publisher->worker_started) {
    uv_mutex_lock(&publisher->queue_mutex);
    publisher->stopping = true;
    uv_mutex_unlock(&publisher->queue_mutex);
    (void)uv_async_send(&publisher->work_async);
    (void)uv_thread_join(&publisher->worker_thread);
    publisher->worker_started = false;
  }
  publisher->loop_initialized = false;
  publisher->work_async_initialized = false;
  publisher->retry_timer_initialized = false;
}
