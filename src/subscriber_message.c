#include "websocket-client/subscriber_internal.h"

#include "app/ethereum_quantity.h"
#include "parson/parson.h"
#include "ulog/ulog.h"

#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

/*
 * Untrusted WebSocket message-processing layer.
 *
 * Subscription notifications can contain a full transaction or only a hash.
 * Hash-only notifications are correlated with bounded follow-up requests.
 * Validated addresses are checked in Redis before an event is published.
 */
constexpr size_t WS_SUBSCRIBER_ADDRESS_CAPACITY = 128;
constexpr size_t WS_SUBSCRIBER_MAX_LOOKUP_MEMBERS = 2;
constexpr size_t WS_SUBSCRIBER_TX_LOOKUP_REQUEST_CAPACITY = 256;
constexpr uint32_t WS_SUBSCRIBER_REDIS_RETRY_INITIAL_BACKOFF_MS = 1'000;
constexpr uint32_t WS_SUBSCRIBER_REDIS_RETRY_MAX_BACKOFF_MS = 60'000;
constexpr int WS_SUBSCRIBER_RPC_INTERNAL_ERROR_CODE = -32603;

[[nodiscard]] static uint64_t ws_subscriber_now_milliseconds() {
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

[[nodiscard]] static bool
ws_subscriber_retry_gate_allows_attempt(ws_subscriber_retry_gate_t *gate,
                                        const char *operation_name) {
  if (gate == nullptr) {
    return true;
  }

  uint64_t now_ms = ws_subscriber_now_milliseconds();
  if (gate->next_retry_at_ms != 0 && now_ms < gate->next_retry_at_ms) {
    if (gate->skipped_attempts < UINT32_MAX) {
      gate->skipped_attempts += 1;
    }
    return false;
  }

  if (gate->skipped_attempts > 0 && operation_name != nullptr) {
    ulog_info("%s retrying after cooldown (skipped=%u)", operation_name,
              (unsigned)gate->skipped_attempts);
    gate->skipped_attempts = 0;
  }

  return true;
}

static void
ws_subscriber_retry_gate_record_failure(ws_subscriber_retry_gate_t *gate,
                                        const char *operation_name,
                                        const char *error_message) {
  if (gate == nullptr) {
    return;
  }

  if (gate->backoff_ms == 0) {
    gate->backoff_ms = WS_SUBSCRIBER_REDIS_RETRY_INITIAL_BACKOFF_MS;
  } else if (gate->backoff_ms < WS_SUBSCRIBER_REDIS_RETRY_MAX_BACKOFF_MS) {
    uint64_t doubled_backoff = (uint64_t)gate->backoff_ms * 2U;
    gate->backoff_ms =
        doubled_backoff > WS_SUBSCRIBER_REDIS_RETRY_MAX_BACKOFF_MS
            ? WS_SUBSCRIBER_REDIS_RETRY_MAX_BACKOFF_MS
            : (uint32_t)doubled_backoff;
  }

  gate->next_retry_at_ms = ws_subscriber_now_milliseconds() + gate->backoff_ms;
  ulog_warn("%s unavailable, backing off for %u ms: %s",
            operation_name != nullptr ? operation_name : "Redis operation",
            (unsigned)gate->backoff_ms,
            error_message != nullptr ? error_message : "(unknown)");
}

static void
ws_subscriber_retry_gate_record_success(ws_subscriber_retry_gate_t *gate,
                                        const char *operation_name) {
  if (gate == nullptr) {
    return;
  }

  if (gate->backoff_ms > 0 && operation_name != nullptr) {
    ulog_info("%s recovered", operation_name);
  }
  gate->next_retry_at_ms = 0;
  gate->backoff_ms = 0;
  gate->skipped_attempts = 0;
}

[[nodiscard]] static bool ws_subscriber_is_valid_tx_hash(const char *tx_hash) {
  if (tx_hash == nullptr) {
    return false;
  }

  size_t length = strlen(tx_hash);
  if (length != WS_SUBSCRIBER_TX_HASH_CAPACITY - 1) {
    return false;
  }
  if (tx_hash[0] != '0' || (tx_hash[1] != 'x' && tx_hash[1] != 'X')) {
    return false;
  }

  for (size_t i = 2; i < length; ++i) {
    if (!isxdigit((unsigned char)tx_hash[i])) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] static bool
ws_subscriber_parse_numeric_id(const JSON_Object *object, uint64_t *out_id) {
  if (object == nullptr || out_id == nullptr) {
    return false;
  }

  auto id_value = json_object_get_value(object, "id");
  if (id_value == nullptr || json_value_get_type(id_value) != JSONNumber) {
    return false;
  }

  double id_number = json_value_get_number(id_value);
  if (id_number < 0 || id_number > (double)UINT64_MAX) {
    return false;
  }

  uint64_t id = (uint64_t)id_number;
  if ((double)id != id_number) {
    return false;
  }

  *out_id = id;
  return true;
}

[[nodiscard]] static ws_subscriber_pending_tx_lookup_t *
ws_subscriber_find_pending_tx_lookup(
    ws_subscriber_runtime_config_t *runtime_config, uint64_t request_id) {
  if (runtime_config == nullptr) {
    return nullptr;
  }

  for (size_t i = 0; i < WS_SUBSCRIBER_MAX_PENDING_TX_LOOKUPS; ++i) {
    ws_subscriber_pending_tx_lookup_t *lookup =
        &runtime_config->pending_tx_lookups[i];
    if (lookup->in_use && lookup->request_id == request_id) {
      return lookup;
    }
  }

  return nullptr;
}

[[nodiscard]] static ws_subscriber_pending_tx_lookup_t *
ws_subscriber_reserve_pending_tx_lookup(
    ws_subscriber_runtime_config_t *runtime_config, uint64_t request_id,
    const char *tx_hash) {
  if (runtime_config == nullptr || tx_hash == nullptr) {
    return nullptr;
  }

  if (runtime_config->pending_tx_lookup_count >=
      WS_SUBSCRIBER_MAX_PENDING_TX_LOOKUPS) {
    return nullptr;
  }

  for (size_t i = 0; i < WS_SUBSCRIBER_MAX_PENDING_TX_LOOKUPS; ++i) {
    ws_subscriber_pending_tx_lookup_t *lookup =
        &runtime_config->pending_tx_lookups[i];
    if (lookup->in_use) {
      continue;
    }

    lookup->in_use = true;
    lookup->request_id = request_id;
    /* Validation guarantees a complete fixed-size hash including its NUL. */
    memcpy(lookup->tx_hash, tx_hash, WS_SUBSCRIBER_TX_HASH_CAPACITY);
    runtime_config->pending_tx_lookup_count += 1;
    return lookup;
  }

  return nullptr;
}

static void ws_subscriber_release_pending_tx_lookup(
    ws_subscriber_runtime_config_t *runtime_config,
    ws_subscriber_pending_tx_lookup_t *lookup) {
  if (runtime_config == nullptr || lookup == nullptr || !lookup->in_use) {
    return;
  }

  lookup->in_use = false;
  lookup->request_id = 0;
  lookup->tx_hash[0] = '\0';
  if (runtime_config->pending_tx_lookup_count > 0) {
    runtime_config->pending_tx_lookup_count -= 1;
  }
}

[[nodiscard]] static bool ws_subscriber_request_transaction_lookup(
    const char *tx_hash, ws_subscriber_runtime_config_t *runtime_config) {
  if (!ws_subscriber_is_valid_tx_hash(tx_hash)) {
    ulog_warn("Skipping hash lookup for invalid transaction hash: %s",
              tx_hash != nullptr ? tx_hash : "(null)");
    return true;
  }

  if (runtime_config == nullptr || runtime_config->client == nullptr) {
    ulog_debug("newPendingTransactions hash=%s (no tx object to inspect)",
               tx_hash);
    return true;
  }

  /* Reserve correlation state before sending the follow-up request. */
  uint64_t request_id = runtime_config->next_tx_lookup_request_id;
  runtime_config->next_tx_lookup_request_id += 1;
  if (runtime_config->next_tx_lookup_request_id == 0) {
    runtime_config->next_tx_lookup_request_id =
        WS_SUBSCRIBER_INITIAL_TX_LOOKUP_REQUEST_ID;
  }

  ws_subscriber_pending_tx_lookup_t *lookup =
      ws_subscriber_reserve_pending_tx_lookup(runtime_config, request_id,
                                              tx_hash);
  if (lookup == nullptr) {
    ulog_warn("Skipping hash lookup for %s because pending lookup queue is "
              "full (%zu)",
              tx_hash, runtime_config->pending_tx_lookup_count);
    return true;
  }

  char request[WS_SUBSCRIBER_TX_LOOKUP_REQUEST_CAPACITY] = {0};
  int request_length =
      snprintf(request, sizeof(request),
               "{\"jsonrpc\":\"2.0\",\"id\":%" PRIu64
               ",\"method\":\"eth_getTransactionByHash\",\"params\":[\"%s\"]}",
               request_id, tx_hash);
  if (request_length < 0 || (size_t)request_length >= sizeof(request)) {
    ws_subscriber_release_pending_tx_lookup(runtime_config, lookup);
    ulog_warn("Skipping hash lookup for %s because request JSON is too large",
              tx_hash);
    return true;
  }

  if (ws_client_send_text(runtime_config->client, request,
                          (size_t)request_length) != WS_STATUS_OK) {
    ws_subscriber_release_pending_tx_lookup(runtime_config, lookup);
    ulog_error(
        "eth_getTransactionByHash send failed for hash=%s (id=%" PRIu64 "): %s",
        tx_hash, request_id, ws_client_last_error(runtime_config->client));
    return false;
  }

  ulog_debug("newPendingTransactions hash=%s (requested tx object, id=%" PRIu64
             ")",
             tx_hash, request_id);
  return true;
}

[[nodiscard]] static bool
ws_subscriber_prepare_member_address(const char *address, char *normalized,
                                     size_t normalized_capacity,
                                     const char **out_member) {
  if (address == nullptr || normalized == nullptr || normalized_capacity == 0 ||
      out_member == nullptr) {
    return false;
  }

  size_t length = strlen(address);
  if (length != 42 || length + 1 > normalized_capacity) {
    return false;
  }

  if (address[0] != '0' || (address[1] != 'x' && address[1] != 'X')) {
    return false;
  }

  bool requires_normalization = address[1] != 'x';
  for (size_t i = 2; i < length; ++i) {
    auto ch = (unsigned char)address[i];
    if (!isxdigit(ch)) {
      return false;
    }
    if (isupper(ch)) {
      requires_normalization = true;
    }
  }

  if (!requires_normalization) {
    *out_member = address;
    return true;
  }

  normalized[0] = '0';
  normalized[1] = 'x';
  for (size_t i = 2; i < length; ++i) {
    normalized[i] = (char)tolower((unsigned char)address[i]);
  }
  normalized[length] = '\0';
  *out_member = normalized;
  return true;
}

[[nodiscard]] static bool
ws_subscriber_decode_sismember_reply(const redisReply *reply,
                                     const char *set_key, const char *member,
                                     bool *out_member_present) {
  if (reply == nullptr || set_key == nullptr || member == nullptr ||
      out_member_present == nullptr) {
    return false;
  }

  bool ok = true;
  if (reply->type == REDIS_REPLY_INTEGER || reply->type == REDIS_REPLY_BOOL) {
    *out_member_present = reply->integer != 0;
  } else if (reply->type == REDIS_REPLY_ERROR) {
    ulog_error("Redis error reply for SISMEMBER (set=%s member=%s): %s",
               set_key, member, reply->str != nullptr ? reply->str : "(null)");
    ok = false;
  } else {
    ulog_error(
        "Unexpected Redis reply type for SISMEMBER (set=%s member=%s): %d",
        set_key, member, reply->type);
    ok = false;
  }

  return ok;
}

/* Execute one Redis membership batch and validate every returned element. */
[[nodiscard]] static bool ws_subscriber_redis_sismember_batch_once(
    redisContext *redis, const char *set_key, const char *const *members,
    size_t member_count, bool *out_member_present) {
  if (redis == nullptr || set_key == nullptr || members == nullptr ||
      out_member_present == nullptr || member_count == 0 ||
      member_count > WS_SUBSCRIBER_MAX_LOOKUP_MEMBERS) {
    return false;
  }

  const char *argv[3] = {
      "SISMEMBER",
      set_key,
      nullptr,
  };
  size_t argvlen[3] = {
      sizeof("SISMEMBER") - 1,
      strlen(set_key),
      0,
  };

  for (size_t i = 0; i < member_count; ++i) {
    argv[2] = members[i];
    argvlen[2] = strlen(members[i]);
    if (redisAppendCommandArgv(redis, 3, argv, argvlen) != REDIS_OK) {
      return false;
    }
  }

  bool ok = true;
  for (size_t i = 0; i < member_count; ++i) {
    void *reply_object = nullptr;
    if (redisGetReply(redis, &reply_object) != REDIS_OK ||
        reply_object == nullptr) {
      if (reply_object != nullptr) {
        freeReplyObject(reply_object);
      }
      return false;
    }

    redisReply *reply = reply_object;
    if (!ws_subscriber_decode_sismember_reply(reply, set_key, members[i],
                                              &out_member_present[i])) {
      ok = false;
    }
    freeReplyObject(reply);
  }

  return ok;
}

[[nodiscard]] static bool ws_subscriber_redis_sismember_batch(
    redisContext *redis, const char *set_key, const char *const *members,
    size_t member_count, bool *out_member_present) {
  if (redis == nullptr || set_key == nullptr || members == nullptr ||
      out_member_present == nullptr || member_count == 0 ||
      member_count > WS_SUBSCRIBER_MAX_LOOKUP_MEMBERS) {
    return false;
  }

  if (ws_subscriber_redis_sismember_batch_once(
          redis, set_key, members, member_count, out_member_present)) {
    return true;
  }

  if (redisReconnect(redis) != REDIS_OK) {
    return false;
  }

  return ws_subscriber_redis_sismember_batch_once(
      redis, set_key, members, member_count, out_member_present);
}

static void
ws_subscriber_report_transaction_match(const char *tx_hash, const char *from,
                                       const char *to, const char *value,
                                       bool from_monitored, bool to_monitored) {
  char amount_eth[APP_ETH_AMOUNT_CAPACITY] = {0};
  const char *formatted_amount =
      app_format_wei_as_eth(value, amount_eth, sizeof(amount_eth))
          ? amount_eth
          : "(unknown)";
  ulog_info("Monitored transaction: hash=%s amount_eth=%s value_wei=%s "
            "from=%s%s to=%s%s",
            tx_hash != nullptr ? tx_hash : "(unknown)", formatted_amount,
            value != nullptr ? value : "(unknown)",
            from != nullptr ? from : "(none)",
            from_monitored ? " [monitored]" : "", to != nullptr ? to : "(none)",
            to_monitored ? " [monitored]" : "");
}

[[nodiscard]] static bool
ws_subscriber_json_set_optional_string(JSON_Object *object, const char *name,
                                       const char *value) {
  if (value != nullptr) {
    return json_object_set_string(object, name, value) == JSONSuccess;
  }
  return json_object_set_null(object, name) == JSONSuccess;
}

/* Construct a stable event envelope before handing it to the publisher. */
static void ws_subscriber_publish_transaction_match(
    const JSON_Object *tx, const char *tx_hash, const char *from,
    const char *to, bool from_monitored, bool to_monitored,
    const ws_subscriber_runtime_config_t *runtime_config) {
  if (tx == nullptr || runtime_config == nullptr ||
      runtime_config->rabbitmq_publisher == nullptr) {
    return;
  }

  auto event_value = json_value_init_object();
  if (event_value == nullptr) {
    ulog_error("Failed to allocate RabbitMQ message JSON object");
    return;
  }

  auto event = json_value_get_object(event_value);
  if (event == nullptr) {
    json_value_free(event_value);
    ulog_error("Failed to get RabbitMQ message JSON object");
    return;
  }

  bool ok = true;
  ok = ok && ws_subscriber_json_set_optional_string(event, "hash", tx_hash);
  ok = ok && ws_subscriber_json_set_optional_string(event, "from", from);
  ok = ok && ws_subscriber_json_set_optional_string(event, "to", to);
  ok = ok && json_object_set_boolean(event, "from_monitored", from_monitored) ==
                 JSONSuccess;
  ok = ok && json_object_set_boolean(event, "to_monitored", to_monitored) ==
                 JSONSuccess;

  JSON_Value *tx_copy =
      json_value_deep_copy(json_object_get_wrapping_value(tx));
  if (ok && tx_copy != nullptr) {
    ok = json_object_set_value(event, "transaction", tx_copy) == JSONSuccess;
  } else {
    ok = false;
  }

  if (!ok) {
    if (tx_copy != nullptr) {
      json_value_free(tx_copy);
    }
    json_value_free(event_value);
    ulog_error("Failed to build RabbitMQ message payload");
    return;
  }

  char *payload = json_serialize_to_string(event_value);
  if (payload == nullptr) {
    json_value_free(event_value);
    ulog_error("Failed to serialize RabbitMQ message payload");
    return;
  }

  auto publish_status = ws_rabbitmq_publisher_publish(
      runtime_config->rabbitmq_publisher, payload, strlen(payload));
  if (publish_status == WS_RABBITMQ_STATUS_OK) {
    ulog_debug("Published monitored transaction %s to RabbitMQ",
               tx_hash != nullptr ? tx_hash : "(unknown)");
  } else {
    ulog_error("Failed to queue monitored transaction %s: %s",
               tx_hash != nullptr ? tx_hash : "(unknown)",
               ws_rabbitmq_status_string(publish_status));
  }

  json_free_serialized_string(payload);
  json_value_free(event_value);
}

/* Normalize a full transaction and evaluate its from/to addresses together. */
static void ws_subscriber_handle_transaction_object(
    const JSON_Object *tx, ws_subscriber_runtime_config_t *runtime_config) {
  if (tx == nullptr) {
    return;
  }

  auto tx_hash = json_object_get_string(tx, "hash");
  auto from = json_object_get_string(tx, "from");
  auto to = json_object_get_string(tx, "to");
  auto value = json_object_get_string(tx, "value");

  if (runtime_config == nullptr || runtime_config->redis == nullptr ||
      runtime_config->monitored_set_key == nullptr) {
    ulog_info("newPendingTransactions: hash=%s",
              tx_hash != nullptr ? tx_hash : "(unknown)");
    return;
  }

  bool from_monitored = false;
  bool to_monitored = false;

  const char *lookup_members[WS_SUBSCRIBER_MAX_LOOKUP_MEMBERS] = {nullptr};
  bool lookup_results[WS_SUBSCRIBER_MAX_LOOKUP_MEMBERS] = {false};
  size_t lookup_count = 0;

  char normalized_from[WS_SUBSCRIBER_ADDRESS_CAPACITY] = {0};
  char normalized_to[WS_SUBSCRIBER_ADDRESS_CAPACITY] = {0};
  int from_lookup_index = -1;
  int to_lookup_index = -1;

  bool from_checked = false;
  if (from != nullptr) {
    const char *from_member = nullptr;
    if (ws_subscriber_prepare_member_address(
            from, normalized_from, sizeof(normalized_from), &from_member)) {
      from_checked = true;
      from_lookup_index = (int)lookup_count;
      lookup_members[lookup_count++] = from_member;
    } else {
      ulog_warn("Skipping address with invalid format: %s", from);
    }
  }

  bool to_checked = false;
  if (to != nullptr) {
    const char *to_member = nullptr;
    if (ws_subscriber_prepare_member_address(
            to, normalized_to, sizeof(normalized_to), &to_member)) {
      to_checked = true;
      if (from_lookup_index >= 0 &&
          strcmp(to_member, lookup_members[(size_t)from_lookup_index]) == 0) {
        to_lookup_index = from_lookup_index;
      } else {
        to_lookup_index = (int)lookup_count;
        lookup_members[lookup_count++] = to_member;
      }
    } else {
      ulog_warn("Skipping address with invalid format: %s", to);
    }
  }

  if (lookup_count > 0) {
    constexpr char WS_SUBSCRIBER_REDIS_OPERATION_NAME[] =
        "Redis SISMEMBER lookup";
    if (ws_subscriber_retry_gate_allows_attempt(
            &runtime_config->redis_retry_gate,
            WS_SUBSCRIBER_REDIS_OPERATION_NAME)) {
      if (ws_subscriber_redis_sismember_batch(
              runtime_config->redis, runtime_config->monitored_set_key,
              lookup_members, lookup_count, lookup_results)) {
        ws_subscriber_retry_gate_record_success(
            &runtime_config->redis_retry_gate,
            WS_SUBSCRIBER_REDIS_OPERATION_NAME);
        if (from_lookup_index >= 0) {
          from_monitored = lookup_results[(size_t)from_lookup_index];
        }
        if (to_lookup_index >= 0) {
          to_monitored = lookup_results[(size_t)to_lookup_index];
        }
      } else {
        ws_subscriber_retry_gate_record_failure(
            &runtime_config->redis_retry_gate,
            WS_SUBSCRIBER_REDIS_OPERATION_NAME, runtime_config->redis->errstr);
        from_checked = false;
        to_checked = false;
        from_monitored = false;
        to_monitored = false;
      }
    } else {
      from_checked = false;
      to_checked = false;
      from_monitored = false;
      to_monitored = false;
    }
  }

  if ((from_checked && from_monitored) || (to_checked && to_monitored)) {
    ws_subscriber_report_transaction_match(tx_hash, from, to, value,
                                           from_monitored, to_monitored);
    ws_subscriber_publish_transaction_match(
        tx, tx_hash, from, to, from_monitored, to_monitored, runtime_config);
  } else if (tx_hash != nullptr) {
    ulog_debug("newPendingTransactions: hash=%s", tx_hash);
  }
}

/* Resolve a follow-up response against its previously reserved hash entry. */
static void ws_subscriber_handle_pending_tx_lookup_response(
    const JSON_Object *object, const ws_subscriber_pending_tx_lookup_t *lookup,
    ws_subscriber_runtime_config_t *runtime_config) {
  if (object == nullptr || lookup == nullptr) {
    return;
  }

  auto error = json_object_get_object(object, "error");
  if (error != nullptr) {
    auto code_value = json_object_get_value(error, "code");
    auto error_message = json_object_get_string(error, "message");
    if (code_value != nullptr &&
        json_value_get_type(code_value) == JSONNumber) {
      auto code = (int)json_object_get_number(error, "code");
      ulog_warn("eth_getTransactionByHash failed for hash=%s (id=%" PRIu64
                ", code=%d, message=%s)",
                lookup->tx_hash, lookup->request_id, code,
                error_message != nullptr ? error_message : "(none)");
    } else {
      ulog_warn("eth_getTransactionByHash failed for hash=%s (id=%" PRIu64
                ") without numeric error code: %s",
                lookup->tx_hash, lookup->request_id,
                error_message != nullptr ? error_message : "(none)");
    }
    return;
  }

  auto result = json_object_get_value(object, "result");
  if (result == nullptr || json_value_get_type(result) == JSONNull) {
    ulog_debug("eth_getTransactionByHash returned null for hash=%s (id=%" PRIu64
               ")",
               lookup->tx_hash, lookup->request_id);
    return;
  }

  if (json_value_get_type(result) != JSONObject) {
    ulog_warn("eth_getTransactionByHash returned unsupported result type (%d) "
              "for hash=%s (id=%" PRIu64 ")",
              json_value_get_type(result), lookup->tx_hash, lookup->request_id);
    return;
  }

  auto tx = json_value_get_object(result);
  ws_subscriber_handle_transaction_object(tx, runtime_config);
}

[[nodiscard]] ws_subscriber_message_action_t
ws_subscriber_handle_message(const char *message, size_t message_length,
                             ws_subscriber_runtime_config_t *runtime_config) {
  /* The receive layer guarantees NUL termination inside its bounded buffer. */
  auto root = json_parse_string(message);
  if (root == nullptr) {
    ulog_warn("Non-JSON websocket message (%zu bytes): %s", message_length,
              message);
    return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
  }

  auto object = json_value_get_object(root);
  if (object == nullptr) {
    ulog_warn("JSON message is not an object: %s", message);
    json_value_free(root);
    return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
  }

  uint64_t response_id = 0;
  if (runtime_config != nullptr &&
      ws_subscriber_parse_numeric_id(object, &response_id)) {
    ws_subscriber_pending_tx_lookup_t *lookup =
        ws_subscriber_find_pending_tx_lookup(runtime_config, response_id);
    if (lookup != nullptr) {
      ws_subscriber_handle_pending_tx_lookup_response(object, lookup,
                                                      runtime_config);
      ws_subscriber_release_pending_tx_lookup(runtime_config, lookup);
      json_value_free(root);
      return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
    }
  }

  auto method = json_object_get_string(object, "method");
  if (method != nullptr && strcmp(method, "eth_subscription") == 0) {
    auto params = json_object_get_object(object, "params");
    auto result =
        (params != nullptr) ? json_object_get_value(params, "result") : nullptr;
    if (result == nullptr) {
      ulog_warn("Subscription notification without result: %s", message);
      json_value_free(root);
      return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
    }

    if (json_value_get_type(result) == JSONString) {
      auto tx_hash = json_value_get_string(result);
      if (!ws_subscriber_request_transaction_lookup(tx_hash, runtime_config)) {
        json_value_free(root);
        return WS_SUBSCRIBER_MESSAGE_ACTION_RECONNECT;
      }
    } else if (json_value_get_type(result) == JSONObject) {
      auto tx = json_value_get_object(result);
      ws_subscriber_handle_transaction_object(tx, runtime_config);
    } else {
      ulog_warn("Subscription result has unsupported type (%d): %s",
                json_value_get_type(result), message);
    }

    json_value_free(root);
    return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
  }

  auto error = json_object_get_object(object, "error");
  if (error != nullptr) {
    auto code_value = json_object_get_value(error, "code");
    auto error_message = json_object_get_string(error, "message");
    if (code_value != nullptr &&
        json_value_get_type(code_value) == JSONNumber) {
      auto code = (int)json_object_get_number(error, "code");
      if (code == WS_SUBSCRIBER_RPC_INTERNAL_ERROR_CODE) {
        ulog_warn("RPC internal error received (id=%.0f, code=%d, message=%s); "
                  "reconnecting",
                  json_object_get_number(object, "id"), code,
                  error_message != nullptr ? error_message : "(none)");
        json_value_free(root);
        return WS_SUBSCRIBER_MESSAGE_ACTION_RECONNECT;
      }
      ulog_warn("RPC error received (id=%.0f, code=%d, message=%s): %s",
                json_object_get_number(object, "id"), code,
                error_message != nullptr ? error_message : "(none)", message);
    } else {
      ulog_warn("RPC error without numeric code: %s", message);
    }
    json_value_free(root);
    return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
  }

  auto result = json_object_get_string(object, "result");
  auto id = json_object_get_number(object, "id");
  if (result != nullptr) {
    ulog_info("Subscription acknowledged (id=%.0f, subscription=%s)", id,
              result);
  } else {
    ulog_debug("RPC message: %s", message);
  }

  json_value_free(root);
  return WS_SUBSCRIBER_MESSAGE_ACTION_CONTINUE;
}
