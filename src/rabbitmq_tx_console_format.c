#include "websocket-client/rabbitmq_tx_console_internal.h"
#include "parson/parson.h"
#include "ulog/ulog.h"
#include <inttypes.h>
#include <stdckdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

constexpr uint64_t APP_WEI_PER_GWEI = 1'000'000'000ULL;
constexpr uint64_t APP_WEI_PER_ETH = 1'000'000'000'000'000'000ULL;

/* Broker-controlled JSON is bounded, parsed, and type-checked before use. */

[[nodiscard]] static const char *app_safe_string(const char *value) {
  return value != nullptr ? value : "(null)";
}

[[nodiscard]] static const char *
app_json_get_nullable_string(const JSON_Object *object, const char *name) {
  if (object == nullptr || name == nullptr) {
    return nullptr;
  }

  JSON_Value *value = json_object_get_value(object, name);
  if (value == nullptr) {
    return nullptr;
  }

  JSON_Value_Type type = json_value_get_type(value);
  if (type == JSONString) {
    return json_value_get_string(value);
  }
  if (type == JSONNull) {
    return nullptr;
  }

  return nullptr;
}

[[nodiscard]] static bool
app_json_get_boolean_or_default(const JSON_Object *object, const char *name,
                                bool default_value) {
  if (object == nullptr || name == nullptr) {
    return default_value;
  }

  JSON_Value *value = json_object_get_value(object, name);
  if (value == nullptr || json_value_get_type(value) != JSONBoolean) {
    return default_value;
  }

  return json_value_get_boolean(value) == JSONBooleanTrue;
}

[[nodiscard]] static bool app_hex_digit_value(char ch, uint8_t *out_digit) {
  if (out_digit == nullptr) {
    return false;
  }

  if (ch >= '0' && ch <= '9') {
    *out_digit = (uint8_t)(ch - '0');
    return true;
  }
  if (ch >= 'a' && ch <= 'f') {
    *out_digit = (uint8_t)(10 + ch - 'a');
    return true;
  }
  if (ch >= 'A' && ch <= 'F') {
    *out_digit = (uint8_t)(10 + ch - 'A');
    return true;
  }

  return false;
}

[[nodiscard]] static bool app_parse_hex_u64(const char *text,
                                            uint64_t *out_value) {
  if (text == nullptr || out_value == nullptr || text[0] != '0' ||
      (text[1] != 'x' && text[1] != 'X')) {
    return false;
  }

  if (text[2] == '\0') {
    *out_value = 0;
    return true;
  }

  uint64_t value = 0;
  for (size_t i = 2; text[i] != '\0'; ++i) {
    uint8_t digit = 0;
    if (!app_hex_digit_value(text[i], &digit)) {
      return false;
    }

    if (value > (UINT64_MAX - digit) / 16U) {
      return false;
    }
    value = value * 16U + digit;
  }

  *out_value = value;
  return true;
}

static void app_format_scaled_u64(uint64_t value, uint64_t scale,
                                  unsigned fractional_digits, char *buffer,
                                  size_t buffer_capacity) {
  if (buffer == nullptr || buffer_capacity == 0 || scale == 0) {
    return;
  }

  uint64_t integer_part = value / scale;
  uint64_t fractional_part = value % scale;
  if (fractional_digits == 0 || fractional_part == 0) {
    (void)snprintf(buffer, buffer_capacity, "%" PRIu64, integer_part);
    return;
  }

  char fractional_text[32] = {0};
  (void)snprintf(fractional_text, sizeof(fractional_text), "%0*" PRIu64,
                 (int)fractional_digits, fractional_part);

  size_t end = strlen(fractional_text);
  while (end > 0 && fractional_text[end - 1] == '0') {
    fractional_text[end - 1] = '\0';
    --end;
  }

  if (end == 0) {
    (void)snprintf(buffer, buffer_capacity, "%" PRIu64, integer_part);
    return;
  }

  (void)snprintf(buffer, buffer_capacity, "%" PRIu64 ".%s", integer_part,
                 fractional_text);
}

static void app_print_hex_line(const char *label, const char *value) {
  if (value == nullptr) {
    printf("%-24s %s\n", label, "(null)");
    return;
  }

  uint64_t parsed = 0;
  if (app_parse_hex_u64(value, &parsed)) {
    printf("%-24s %s (%" PRIu64 ")\n", label, value, parsed);
  } else {
    printf("%-24s %s\n", label, value);
  }
}

static void app_print_wei_line(const char *label, const char *value,
                               bool as_eth) {
  if (value == nullptr) {
    printf("%-24s %s\n", label, "(null)");
    return;
  }

  uint64_t parsed = 0;
  if (!app_parse_hex_u64(value, &parsed)) {
    printf("%-24s %s\n", label, value);
    return;
  }

  char formatted[96] = {0};
  if (as_eth) {
    app_format_scaled_u64(parsed, APP_WEI_PER_ETH, 18, formatted,
                          sizeof(formatted));
    printf("%-24s %s (%" PRIu64 " wei, %s ETH)\n", label, value, parsed,
           formatted);
  } else {
    app_format_scaled_u64(parsed, APP_WEI_PER_GWEI, 9, formatted,
                          sizeof(formatted));
    printf("%-24s %s (%" PRIu64 " wei, %s Gwei)\n", label, value, parsed,
           formatted);
  }
}

static void app_print_transaction_summary(const JSON_Object *event) {
  if (event == nullptr) {
    return;
  }

  const char *hash = app_json_get_nullable_string(event, "hash");
  const char *from = app_json_get_nullable_string(event, "from");
  const char *to = app_json_get_nullable_string(event, "to");
  bool from_monitored =
      app_json_get_boolean_or_default(event, "from_monitored", false);
  bool to_monitored =
      app_json_get_boolean_or_default(event, "to_monitored", false);

  const JSON_Object *transaction = json_object_get_object(event, "transaction");
  if (transaction == nullptr) {
    JSON_Value *tx_value = json_object_get_value(event, "transaction");
    if (tx_value != nullptr && json_value_get_type(tx_value) == JSONObject) {
      transaction = json_value_get_object(tx_value);
    }
  }

  if (hash == nullptr && transaction != nullptr) {
    hash = app_json_get_nullable_string(transaction, "hash");
  }

  printf("\n============================================================\n");
  printf("Monitored Transaction\n");
  printf("============================================================\n");
  printf("%-24s %s\n", "Hash", app_safe_string(hash));
  printf("%-24s %s [%s]\n", "From", app_safe_string(from),
         from_monitored ? "monitored" : "not monitored");
  printf("%-24s %s [%s]\n", "To", app_safe_string(to),
         to_monitored ? "monitored" : "not monitored");

  if (transaction == nullptr) {
    printf("%-24s %s\n", "Transaction", "(missing or invalid)");
    fflush(stdout);
    return;
  }

  const char *chain_id = app_json_get_nullable_string(transaction, "chainId");
  const char *nonce = app_json_get_nullable_string(transaction, "nonce");
  const char *tx_type = app_json_get_nullable_string(transaction, "type");
  const char *tx_to = app_json_get_nullable_string(transaction, "to");
  const char *input = app_json_get_nullable_string(transaction, "input");
  const char *gas = app_json_get_nullable_string(transaction, "gas");
  const char *gas_price = app_json_get_nullable_string(transaction, "gasPrice");
  const char *max_fee_per_gas =
      app_json_get_nullable_string(transaction, "maxFeePerGas");
  const char *max_priority_fee_per_gas =
      app_json_get_nullable_string(transaction, "maxPriorityFeePerGas");
  const char *value = app_json_get_nullable_string(transaction, "value");

  app_print_hex_line("Chain ID", chain_id);
  app_print_hex_line("Nonce", nonce);
  printf("%-24s %s\n", "Type", app_safe_string(tx_type));
  printf("%-24s %s\n", "Tx To", app_safe_string(tx_to));
  app_print_hex_line("Gas Limit", gas);
  app_print_wei_line("Gas Price", gas_price, false);
  app_print_wei_line("Max Fee Per Gas", max_fee_per_gas, false);
  app_print_wei_line("Max Priority Fee", max_priority_fee_per_gas, false);
  app_print_wei_line("Value", value, true);
  printf("%-24s %s\n", "Input", app_safe_string(input));

  fflush(stdout);
}

void app_handle_payload(const void *body, size_t body_length) {
  if (body_length > 0 && body == nullptr) {
    ulog_error("Skipping message with null payload pointer\n");
    return;
  }

  size_t payload_capacity = 0;
  if (ckd_add(&payload_capacity, body_length, (size_t)1)) {
    ulog_error("Out of memory while copying RabbitMQ payload (%zu bytes)\n",
               body_length);
    return;
  }

  char *payload = calloc(payload_capacity, sizeof(char));
  if (payload == nullptr) {
    ulog_error("Out of memory while copying RabbitMQ payload (%zu bytes)\n",
               body_length);
    return;
  }

  if (body_length > 0) {
    memcpy(payload, body, body_length);
  }

  JSON_Value *root = json_parse_string(payload);
  if (root == nullptr) {
    ulog_error("Failed to parse RabbitMQ message as JSON (%zu bytes): %s\n",
               body_length, payload);
    free(payload);
    return;
  }

  JSON_Object *event = json_value_get_object(root);
  if (event == nullptr) {
    ulog_error("RabbitMQ message JSON is not an object: %s\n", payload);
    json_value_free(root);
    free(payload);
    return;
  }

  app_print_transaction_summary(event);

  json_value_free(root);
  free(payload);
}
