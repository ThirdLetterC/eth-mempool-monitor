#pragma once

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdckdint.h>
#include <string.h>

constexpr size_t APP_UINT256_DECIMAL_DIGITS = 78;
constexpr size_t APP_FORMATTED_QUANTITY_CAPACITY = 80;

[[nodiscard]] static inline bool app_ethereum_hex_digit(char ch,
                                                        uint8_t *out_digit) {
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

[[nodiscard]] static inline bool app_format_wei_quantity(const char *wei_hex,
                                                         size_t decimal_places,
                                                         char *out,
                                                         size_t out_capacity) {
  if (wei_hex == nullptr || out == nullptr ||
      out_capacity < APP_FORMATTED_QUANTITY_CAPACITY || decimal_places == 0 ||
      decimal_places > 18 || wei_hex[0] != '0' ||
      (wei_hex[1] != 'x' && wei_hex[1] != 'X')) {
    return false;
  }

  const size_t input_length = strlen(wei_hex);
  if (input_length < 3 || input_length > 66) {
    return false;
  }

  /* Little-endian base-10 digits avoid lossy floating-point conversion. */
  uint8_t decimal_digits[APP_UINT256_DECIMAL_DIGITS] = {0};
  size_t decimal_length = 1;
  for (size_t i = 2; i < input_length; ++i) {
    uint8_t hex_digit = 0;
    if (!app_ethereum_hex_digit(wei_hex[i], &hex_digit)) {
      return false;
    }

    uint16_t carry = hex_digit;
    for (size_t j = 0; j < decimal_length; ++j) {
      const uint16_t expanded = (uint16_t)decimal_digits[j] * 16U + carry;
      decimal_digits[j] = (uint8_t)(expanded % 10U);
      carry = expanded / 10U;
    }
    while (carry > 0) {
      if (decimal_length >= APP_UINT256_DECIMAL_DIGITS) {
        return false;
      }
      decimal_digits[decimal_length++] = (uint8_t)(carry % 10U);
      carry /= 10U;
    }
  }

  char decimal[APP_UINT256_DECIMAL_DIGITS + 1] = {0};
  for (size_t i = 0; i < decimal_length; ++i) {
    decimal[i] = (char)('0' + decimal_digits[decimal_length - i - 1]);
  }

  size_t output_length = 0;
  size_t decimal_point = 0;
  if (decimal_length <= decimal_places) {
    const size_t leading_zeroes = decimal_places - decimal_length;
    out[output_length++] = '0';
    decimal_point = output_length;
    out[output_length++] = '.';
    memset(out + output_length, '0', leading_zeroes);
    output_length += leading_zeroes;
    memcpy(out + output_length, decimal, decimal_length);
    output_length += decimal_length;
  } else {
    const size_t integer_length = decimal_length - decimal_places;
    memcpy(out, decimal, integer_length);
    output_length = integer_length;
    decimal_point = output_length;
    out[output_length++] = '.';
    memcpy(out + output_length, decimal + integer_length, decimal_places);
    output_length += decimal_places;
  }

  while (output_length > decimal_point + 1 && out[output_length - 1] == '0') {
    --output_length;
  }
  if (output_length == decimal_point + 1) {
    output_length = decimal_point;
  }
  out[output_length] = '\0';
  return true;
}

/** Format a hexadecimal uint256 wei quantity as an exact decimal ETH value. */
[[nodiscard]] static inline bool
app_format_wei_as_eth(const char *wei_hex, char *out, size_t out_capacity) {
  return app_format_wei_quantity(wei_hex, 18, out, out_capacity);
}

/** Format a hexadecimal uint256 wei quantity as an exact decimal Gwei value. */
[[nodiscard]] static inline bool
app_format_wei_as_gwei(const char *wei_hex, char *out, size_t out_capacity) {
  return app_format_wei_quantity(wei_hex, 9, out, out_capacity);
}

[[nodiscard]] static inline bool
app_parse_hex_u64_quantity(const char *hex, uint64_t *out_value) {
  if (hex == nullptr || out_value == nullptr || hex[0] != '0' ||
      (hex[1] != 'x' && hex[1] != 'X')) {
    return false;
  }

  const size_t input_length = strlen(hex);
  if (input_length < 3 || input_length > 18) {
    return false;
  }

  uint64_t value = 0;
  for (size_t i = 2; i < input_length; ++i) {
    uint8_t digit = 0;
    if (!app_ethereum_hex_digit(hex[i], &digit) ||
        value > (UINT64_MAX - digit) / 16U) {
      return false;
    }
    value = value * 16U + digit;
  }

  *out_value = value;
  return true;
}

/** Format gas limit times fee cap as ETH, rejecting uint64 overflow. */
[[nodiscard]] static inline bool
app_format_max_fee_as_eth(const char *gas_hex, const char *fee_cap_hex,
                          char *out, size_t out_capacity) {
  uint64_t gas = 0;
  uint64_t fee_cap = 0;
  uint64_t fee_wei = 0;
  if (!app_parse_hex_u64_quantity(gas_hex, &gas) ||
      !app_parse_hex_u64_quantity(fee_cap_hex, &fee_cap) ||
      ckd_mul(&fee_wei, gas, fee_cap)) {
    return false;
  }

  char fee_hex[2 + 16 + 1] = {0};
  const int written = snprintf(fee_hex, sizeof(fee_hex), "0x%" PRIx64, fee_wei);
  if (written < 0 || (size_t)written >= sizeof(fee_hex)) {
    return false;
  }
  return app_format_wei_as_eth(fee_hex, out, out_capacity);
}
