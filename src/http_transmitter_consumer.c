#include "websocket-client/http_transmitter_internal.h"
#include "rabbitmq/framing.h"
#include "rabbitmq/tcp_socket.h"
#include "ulog/ulog.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

constexpr uint64_t HTTP_STATS_REPORT_INTERVAL = 100;

typedef enum http_work_state : uint8_t {
  HTTP_WORK_FREE = 0,
  HTTP_WORK_QUEUED,
  HTTP_WORK_RUNNING,
  HTTP_WORK_COMPLETED,
} http_work_state_t;

typedef struct http_work_slot http_work_slot_t;
struct http_work_slot {
  http_work_state_t state;
  uint64_t delivery_tag;
  unsigned char *body;
  size_t body_length;
  http_delivery_result_t result;
};

typedef struct http_worker_pool http_worker_pool_t;

typedef struct http_worker_context http_worker_context_t;
struct http_worker_context {
  http_worker_pool_t *pool;
  size_t client_index;
};

struct http_worker_pool {
  pthread_mutex_t mutex;
  pthread_cond_t work_available;
  pthread_cond_t completion_available;
  bool mutex_initialized;
  bool work_condition_initialized;
  bool completion_condition_initialized;
  bool stopping;
  bool failed;
  http_work_slot_t *slots;
  http_webhook_client_t *clients;
  pthread_t *threads;
  http_worker_context_t *contexts;
  size_t slot_count;
  size_t clients_initialized;
  size_t threads_started;
  size_t outstanding;
};

typedef struct http_completion http_completion_t;
struct http_completion {
  uint64_t delivery_tag;
  http_delivery_result_t result;
};

typedef struct http_transmitter_stats http_transmitter_stats_t;
struct http_transmitter_stats {
  uint64_t total;
  uint64_t sent;
  uint64_t rejected;
  uint64_t requeued;
  uint64_t settlements;
};

static void http_log_rpc_failure(const char *action, amqp_rpc_reply_t reply) {
  if (reply.reply_type == AMQP_RESPONSE_NONE) {
    ulog_error("RabbitMQ %s failed: missing RPC reply", action);
  } else if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
    ulog_error("RabbitMQ %s failed: %s", action,
               amqp_error_string2(reply.library_error));
  } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
             reply.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
    const amqp_connection_close_t *close =
        (const amqp_connection_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: connection close %u (%.*s)", action,
               close->reply_code, (int)close->reply_text.len,
               (const char *)close->reply_text.bytes);
  } else if (reply.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION &&
             reply.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
    const amqp_channel_close_t *close =
        (const amqp_channel_close_t *)reply.reply.decoded;
    ulog_error("RabbitMQ %s failed: channel close %u (%.*s)", action,
               close->reply_code, (int)close->reply_text.len,
               (const char *)close->reply_text.bytes);
  } else {
    ulog_error("RabbitMQ %s failed: unexpected reply type=%d", action,
               reply.reply_type);
  }
}

[[nodiscard]] static bool
http_expect_normal_reply(amqp_connection_state_t connection,
                         const char *action) {
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(connection);
  if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
    return true;
  }
  http_log_rpc_failure(action, reply);
  return false;
}

void http_transmitter_rabbitmq_disconnect(
    http_transmitter_consumer_t *consumer) {
  if (consumer == nullptr || consumer->connection == nullptr) {
    return;
  }
  if (consumer->channel_open) {
    (void)amqp_channel_close(consumer->connection, consumer->channel,
                             AMQP_REPLY_SUCCESS);
    consumer->channel_open = false;
  }
  if (consumer->logged_in) {
    (void)amqp_connection_close(consumer->connection, AMQP_REPLY_SUCCESS);
    consumer->logged_in = false;
  }
  int status = amqp_destroy_connection(consumer->connection);
  if (status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ destroy_connection returned: %s",
               amqp_error_string2(status));
  }
  consumer->connection = nullptr;
}

[[nodiscard]] http_transmitter_status_t
http_transmitter_rabbitmq_connect(const http_transmitter_config_t *config,
                                  http_transmitter_consumer_t *consumer) {
  if (config == nullptr || consumer == nullptr ||
      config->rabbitmq_host == nullptr || config->rabbitmq_port.value == 0 ||
      config->rabbitmq_username == nullptr ||
      config->rabbitmq_password == nullptr ||
      config->rabbitmq_vhost == nullptr || config->rabbitmq_queue == nullptr ||
      config->rabbitmq_channel.value == 0 ||
      config->rabbitmq_heartbeat.value == 0 ||
      config->prefetch_count.value == 0 ||
      config->parallel_requests.value == 0) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  *consumer = (http_transmitter_consumer_t){
      .channel = (amqp_channel_t)config->rabbitmq_channel.value,
  };
  consumer->connection = amqp_new_connection();
  if (consumer->connection == nullptr) {
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  amqp_socket_t *socket = amqp_tcp_socket_new(consumer->connection);
  if (socket == nullptr) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  int socket_status = amqp_socket_open(socket, config->rabbitmq_host,
                                       (int)config->rabbitmq_port.value);
  if (socket_status != AMQP_STATUS_OK) {
    ulog_error("Failed to connect to RabbitMQ at %s:%u: %s",
               config->rabbitmq_host, (unsigned)config->rabbitmq_port.value,
               amqp_error_string2(socket_status));
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_CONNECTION_ERROR;
  }
  amqp_rpc_reply_t login = amqp_login(
      consumer->connection, config->rabbitmq_vhost, 0, AMQP_DEFAULT_FRAME_SIZE,
      (int)config->rabbitmq_heartbeat.value, AMQP_SASL_METHOD_PLAIN,
      config->rabbitmq_username, config->rabbitmq_password);
  if (login.reply_type != AMQP_RESPONSE_NORMAL) {
    http_log_rpc_failure("login", login);
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_CONNECTION_ERROR;
  }
  consumer->logged_in = true;
  if (amqp_channel_open(consumer->connection, consumer->channel) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "channel open")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  consumer->channel_open = true;

  amqp_bytes_t queue = amqp_cstring_bytes(config->rabbitmq_queue);
  amqp_queue_declare_ok_t *declaration = amqp_queue_declare(
      consumer->connection, consumer->channel, queue, false,
      config->rabbitmq_queue_durable, false, false, amqp_empty_table);
  if (declaration == nullptr ||
      !http_expect_normal_reply(consumer->connection, "queue declare")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  consumer->pending_messages_at_connect = declaration->message_count;
  if (amqp_basic_qos(consumer->connection, consumer->channel, 0,
                     config->prefetch_count.value, false) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "basic.qos")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  if (amqp_basic_consume(consumer->connection, consumer->channel, queue,
                         amqp_empty_bytes, false, false, false,
                         amqp_empty_table) == nullptr ||
      !http_expect_normal_reply(consumer->connection, "basic.consume")) {
    http_transmitter_rabbitmq_disconnect(consumer);
    return HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }
  ulog_info("Connected to RabbitMQ at %s:%u (queue=%s, pending=%u, "
            "prefetch=%u)",
            config->rabbitmq_host, (unsigned)config->rabbitmq_port.value,
            config->rabbitmq_queue, consumer->pending_messages_at_connect,
            (unsigned)config->prefetch_count.value);
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] static void *http_allocate_array(size_t count,
                                               size_t element_size) {
  size_t allocation_size = 0;
  if (count == 0 || element_size == 0 ||
      ckd_mul(&allocation_size, count, element_size)) {
    return nullptr;
  }
  return calloc(1, allocation_size);
}

[[nodiscard]] static http_work_slot_t *
http_worker_find_slot(http_worker_pool_t *pool, http_work_state_t state) {
  for (size_t i = 0; i < pool->slot_count; ++i) {
    if (pool->slots[i].state == state) {
      return &pool->slots[i];
    }
  }
  return nullptr;
}

[[nodiscard]] static void *http_worker_main(void *argument) {
  http_worker_context_t *context = argument;
  http_worker_pool_t *pool = context->pool;
  http_webhook_client_t *client = &pool->clients[context->client_index];

  while (true) {
    int lock_status = pthread_mutex_lock(&pool->mutex);
    if (lock_status != 0) {
      ulog_error("Webhook worker mutex lock failed: %s", strerror(lock_status));
      return nullptr;
    }
    http_work_slot_t *slot = http_worker_find_slot(pool, HTTP_WORK_QUEUED);
    while (!pool->stopping && slot == nullptr) {
      int wait_status = pthread_cond_wait(&pool->work_available, &pool->mutex);
      if (wait_status != 0) {
        ulog_error("Webhook worker condition wait failed: %s",
                   strerror(wait_status));
        pool->failed = true;
        pool->stopping = true;
        (void)pthread_cond_broadcast(&pool->work_available);
        (void)pthread_cond_broadcast(&pool->completion_available);
        break;
      }
      slot = http_worker_find_slot(pool, HTTP_WORK_QUEUED);
    }
    if (pool->stopping) {
      (void)pthread_mutex_unlock(&pool->mutex);
      return nullptr;
    }
    slot->state = HTTP_WORK_RUNNING;
    int unlock_status = pthread_mutex_unlock(&pool->mutex);
    if (unlock_status != 0) {
      ulog_error("Webhook worker mutex unlock failed: %s",
                 strerror(unlock_status));
      return nullptr;
    }

    http_delivery_result_t result =
        http_webhook_deliver(client, slot->body, slot->body_length);

    lock_status = pthread_mutex_lock(&pool->mutex);
    if (lock_status != 0) {
      ulog_error("Webhook worker completion lock failed: %s",
                 strerror(lock_status));
      return nullptr;
    }
    slot->result = result;
    slot->state = HTTP_WORK_COMPLETED;
    (void)pthread_cond_signal(&pool->completion_available);
    (void)pthread_mutex_unlock(&pool->mutex);
  }
}

static void http_worker_pool_cleanup(http_worker_pool_t *pool) {
  if (pool == nullptr) {
    return;
  }
  if (pool->mutex_initialized) {
    if (pthread_mutex_lock(&pool->mutex) == 0) {
      pool->stopping = true;
      if (pool->work_condition_initialized) {
        (void)pthread_cond_broadcast(&pool->work_available);
      }
      (void)pthread_mutex_unlock(&pool->mutex);
    }
  }
  for (size_t i = 0; i < pool->threads_started; ++i) {
    int join_status = pthread_join(pool->threads[i], nullptr);
    if (join_status != 0) {
      ulog_error("Webhook worker join failed: %s", strerror(join_status));
    }
  }
  for (size_t i = 0; i < pool->clients_initialized; ++i) {
    http_webhook_client_cleanup(&pool->clients[i]);
  }
  if (pool->slots != nullptr) {
    for (size_t i = 0; i < pool->slot_count; ++i) {
      free(pool->slots[i].body);
    }
  }
  if (pool->completion_condition_initialized) {
    (void)pthread_cond_destroy(&pool->completion_available);
  }
  if (pool->work_condition_initialized) {
    (void)pthread_cond_destroy(&pool->work_available);
  }
  if (pool->mutex_initialized) {
    (void)pthread_mutex_destroy(&pool->mutex);
  }
  free(pool->contexts);
  free(pool->threads);
  free(pool->clients);
  free(pool->slots);
  *pool = (http_worker_pool_t){0};
}

[[nodiscard]] static http_transmitter_status_t
http_worker_pool_init(http_worker_pool_t *pool,
                      const http_transmitter_config_t *config) {
  if (pool == nullptr || config == nullptr ||
      config->parallel_requests.value == 0) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  *pool = (http_worker_pool_t){
      .slot_count = config->parallel_requests.value,
  };
  pool->slots = http_allocate_array(pool->slot_count, sizeof(*pool->slots));
  pool->clients = http_allocate_array(pool->slot_count, sizeof(*pool->clients));
  pool->threads = http_allocate_array(pool->slot_count, sizeof(*pool->threads));
  pool->contexts =
      http_allocate_array(pool->slot_count, sizeof(*pool->contexts));
  if (pool->slots == nullptr || pool->clients == nullptr ||
      pool->threads == nullptr || pool->contexts == nullptr) {
    http_worker_pool_cleanup(pool);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  int status = pthread_mutex_init(&pool->mutex, nullptr);
  if (status != 0) {
    ulog_error("Webhook worker mutex initialization failed: %s",
               strerror(status));
    http_worker_pool_cleanup(pool);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  pool->mutex_initialized = true;
  status = pthread_cond_init(&pool->work_available, nullptr);
  if (status != 0) {
    ulog_error("Webhook work condition initialization failed: %s",
               strerror(status));
    http_worker_pool_cleanup(pool);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  pool->work_condition_initialized = true;
  status = pthread_cond_init(&pool->completion_available, nullptr);
  if (status != 0) {
    ulog_error("Webhook completion condition initialization failed: %s",
               strerror(status));
    http_worker_pool_cleanup(pool);
    return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
  }
  pool->completion_condition_initialized = true;

  for (size_t i = 0; i < pool->slot_count; ++i) {
    http_transmitter_status_t client_status =
        http_webhook_client_init(&pool->clients[i], config);
    if (client_status != HTTP_TRANSMITTER_STATUS_OK) {
      http_worker_pool_cleanup(pool);
      return client_status;
    }
    ++pool->clients_initialized;
  }
  for (size_t i = 0; i < pool->slot_count; ++i) {
    pool->contexts[i] = (http_worker_context_t){
        .pool = pool,
        .client_index = i,
    };
    status = pthread_create(&pool->threads[i], nullptr, http_worker_main,
                            &pool->contexts[i]);
    if (status != 0) {
      ulog_error("Webhook worker creation failed: %s", strerror(status));
      http_worker_pool_cleanup(pool);
      return HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
    }
    ++pool->threads_started;
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] static bool http_worker_pool_enqueue(http_worker_pool_t *pool,
                                                   uint64_t delivery_tag,
                                                   unsigned char *body,
                                                   size_t body_length) {
  if (pthread_mutex_lock(&pool->mutex) != 0) {
    return false;
  }
  http_work_slot_t *slot = http_worker_find_slot(pool, HTTP_WORK_FREE);
  if (slot == nullptr) {
    (void)pthread_mutex_unlock(&pool->mutex);
    return false;
  }
  *slot = (http_work_slot_t){
      .state = HTTP_WORK_QUEUED,
      .delivery_tag = delivery_tag,
      .body = body,
      .body_length = body_length,
  };
  ++pool->outstanding;
  (void)pthread_cond_signal(&pool->work_available);
  (void)pthread_mutex_unlock(&pool->mutex);
  return true;
}

[[nodiscard]] static bool
http_worker_pool_take_completion(http_worker_pool_t *pool, bool wait,
                                 http_completion_t *completion) {
  if (pthread_mutex_lock(&pool->mutex) != 0) {
    return false;
  }
  http_work_slot_t *slot = http_worker_find_slot(pool, HTTP_WORK_COMPLETED);
  while (wait && slot == nullptr && !pool->stopping) {
    if (pthread_cond_wait(&pool->completion_available, &pool->mutex) != 0) {
      pool->failed = true;
      pool->stopping = true;
      (void)pthread_cond_broadcast(&pool->work_available);
      (void)pthread_mutex_unlock(&pool->mutex);
      return false;
    }
    slot = http_worker_find_slot(pool, HTTP_WORK_COMPLETED);
  }
  if (slot == nullptr) {
    (void)pthread_mutex_unlock(&pool->mutex);
    return false;
  }
  *completion = (http_completion_t){
      .delivery_tag = slot->delivery_tag,
      .result = slot->result,
  };
  free(slot->body);
  *slot = (http_work_slot_t){0};
  --pool->outstanding;
  (void)pthread_mutex_unlock(&pool->mutex);
  return true;
}

[[nodiscard]] static bool
http_worker_pool_has_failed(http_worker_pool_t *pool) {
  if (pthread_mutex_lock(&pool->mutex) != 0) {
    return true;
  }
  bool failed = pool->failed;
  (void)pthread_mutex_unlock(&pool->mutex);
  return failed;
}

[[nodiscard]] static http_transmitter_status_t
http_settle_delivery(http_transmitter_consumer_t *consumer,
                     uint64_t delivery_tag, http_delivery_result_t result) {
  int status = AMQP_STATUS_OK;
  if (result == HTTP_DELIVERY_SUCCESS) {
    status = amqp_basic_ack(consumer->connection, consumer->channel,
                            delivery_tag, false);
  } else if (result == HTTP_DELIVERY_PERMANENT_FAILURE) {
    status = amqp_basic_reject(consumer->connection, consumer->channel,
                               delivery_tag, false);
  } else {
    status = amqp_basic_nack(consumer->connection, consumer->channel,
                             delivery_tag, false, true);
  }
  if (status != AMQP_STATUS_OK) {
    ulog_error("RabbitMQ delivery settlement failed: %s",
               amqp_error_string2(status));
    return HTTP_TRANSMITTER_STATUS_ACK_ERROR;
  }
  if (result == HTTP_DELIVERY_TRANSIENT_FAILURE) {
    ulog_warn("Webhook retries exhausted; RabbitMQ delivery requeued");
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}

static void http_counter_increment(uint64_t *counter) {
  uint64_t incremented = 0;
  *counter =
      ckd_add(&incremented, *counter, (uint64_t)1) ? UINT64_MAX : incremented;
}

static void http_stats_log(const http_transmitter_stats_t *stats,
                           size_t in_flight) {
  uint64_t processed = 0;
  if (ckd_add(&processed, stats->sent, stats->rejected)) {
    processed = UINT64_MAX;
  }
  uint64_t remaining = processed < stats->total ? stats->total - processed : 0;
  ulog_info("HTTP transmitter stats: must_send=%" PRIu64 " sent=%" PRIu64
            " remaining=%" PRIu64 " in_flight=%zu rejected=%" PRIu64
            " requeued=%" PRIu64,
            stats->total, stats->sent, remaining, in_flight, stats->rejected,
            stats->requeued);
}

static void http_stats_record_completion(http_transmitter_stats_t *stats,
                                         http_delivery_result_t result) {
  http_counter_increment(&stats->settlements);
  if (result == HTTP_DELIVERY_SUCCESS) {
    http_counter_increment(&stats->sent);
  } else if (result == HTTP_DELIVERY_PERMANENT_FAILURE) {
    http_counter_increment(&stats->rejected);
  } else {
    http_counter_increment(&stats->requeued);
  }
}

static void http_stats_discover_delivery(http_transmitter_stats_t *stats,
                                         size_t outstanding, bool redelivered) {
  if (redelivered) {
    return;
  }
  uint64_t processed = 0;
  uint64_t observed = 0;
  if (ckd_add(&processed, stats->sent, stats->rejected) ||
      ckd_add(&observed, processed, (uint64_t)outstanding) ||
      ckd_add(&observed, observed, (uint64_t)1)) {
    observed = UINT64_MAX;
  }
  if (observed > stats->total) {
    stats->total = observed;
  }
}

[[nodiscard]] static http_transmitter_status_t http_process_completion(
    http_transmitter_consumer_t *consumer, http_worker_pool_t *pool,
    http_transmitter_stats_t *stats, const http_completion_t *completion) {
  http_transmitter_status_t status = http_settle_delivery(
      consumer, completion->delivery_tag, completion->result);
  if (status != HTTP_TRANSMITTER_STATUS_OK) {
    return status;
  }
  http_stats_record_completion(stats, completion->result);
  uint64_t processed = 0;
  bool complete = !ckd_add(&processed, stats->sent, stats->rejected) &&
                  processed >= stats->total && pool->outstanding == 0;
  if (stats->settlements == 1 ||
      stats->settlements % HTTP_STATS_REPORT_INTERVAL == 0 || complete ||
      http_transmitter_is_shutdown_requested()) {
    http_stats_log(stats, pool->outstanding);
  }
  return HTTP_TRANSMITTER_STATUS_OK;
}

[[nodiscard]] http_transmitter_status_t
http_transmitter_consume_loop(http_transmitter_consumer_t *consumer,
                              const http_transmitter_config_t *config) {
  if (consumer == nullptr || config == nullptr ||
      consumer->connection == nullptr) {
    return HTTP_TRANSMITTER_STATUS_INVALID_CONFIG;
  }
  http_worker_pool_t pool = {0};
  http_transmitter_status_t status = http_worker_pool_init(&pool, config);
  if (status != HTTP_TRANSMITTER_STATUS_OK) {
    return status;
  }
  http_transmitter_stats_t stats = {
      .total = consumer->pending_messages_at_connect,
  };
  http_stats_log(&stats, 0);

  struct timeval timeout = {
      .tv_sec = (time_t)config->read_timeout.value,
      .tv_usec = 0,
  };
  while (status == HTTP_TRANSMITTER_STATUS_OK) {
    http_completion_t completion = {0};
    while (http_worker_pool_take_completion(&pool, false, &completion)) {
      status = http_process_completion(consumer, &pool, &stats, &completion);
      if (status != HTTP_TRANSMITTER_STATUS_OK) {
        break;
      }
    }
    if (http_worker_pool_has_failed(&pool)) {
      status = HTTP_TRANSMITTER_STATUS_THREAD_ERROR;
      break;
    }
    if (status != HTTP_TRANSMITTER_STATUS_OK) {
      break;
    }
    if (http_transmitter_is_shutdown_requested()) {
      if (pool.outstanding == 0) {
        break;
      }
      if (http_worker_pool_take_completion(&pool, true, &completion)) {
        status = http_process_completion(consumer, &pool, &stats, &completion);
      }
      continue;
    }
    if (pool.outstanding >= pool.slot_count) {
      if (http_worker_pool_take_completion(&pool, true, &completion)) {
        status = http_process_completion(consumer, &pool, &stats, &completion);
      }
      continue;
    }

    amqp_maybe_release_buffers(consumer->connection);
    amqp_envelope_t envelope = {0};
    amqp_rpc_reply_t reply =
        amqp_consume_message(consumer->connection, &envelope, &timeout, 0);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      if (envelope.message.body.len > HTTP_TRANSMITTER_MAX_PAYLOAD_BYTES) {
        http_stats_discover_delivery(&stats, pool.outstanding,
                                     envelope.redelivered);
        completion = (http_completion_t){
            .delivery_tag = envelope.delivery_tag,
            .result = HTTP_DELIVERY_PERMANENT_FAILURE,
        };
        ulog_error("Rejecting oversized RabbitMQ payload (%zu bytes)",
                   envelope.message.body.len);
        amqp_destroy_envelope(&envelope);
        status = http_process_completion(consumer, &pool, &stats, &completion);
        continue;
      }
      size_t allocation_size = 0;
      if (ckd_add(&allocation_size, envelope.message.body.len, (size_t)1)) {
        amqp_destroy_envelope(&envelope);
        status = HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
        break;
      }
      unsigned char *body = calloc(allocation_size, sizeof(unsigned char));
      if (body == nullptr) {
        amqp_destroy_envelope(&envelope);
        status = HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
        break;
      }
      memcpy(body, envelope.message.body.bytes, envelope.message.body.len);
      http_stats_discover_delivery(&stats, pool.outstanding,
                                   envelope.redelivered);
      bool enqueued = http_worker_pool_enqueue(&pool, envelope.delivery_tag,
                                               body, envelope.message.body.len);
      amqp_destroy_envelope(&envelope);
      if (!enqueued) {
        free(body);
        status = HTTP_TRANSMITTER_STATUS_ALLOCATION_FAILED;
      }
      continue;
    }
    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
        reply.library_error == AMQP_STATUS_TIMEOUT) {
      continue;
    }
    http_log_rpc_failure("consume message", reply);
    status = HTTP_TRANSMITTER_STATUS_PROTOCOL_ERROR;
  }

  http_stats_log(&stats, pool.outstanding);
  http_worker_pool_cleanup(&pool);
  return status;
}
