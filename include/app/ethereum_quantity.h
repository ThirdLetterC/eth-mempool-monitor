#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

constexpr size_t APP_UINT256_DECIMAL_DIGITS = 78;
constexpr size_t APP_ETH_AMOUNT_CAPACITY = 80;

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

/** Format a hexadecimal uint256 wei quantity as an exact decimal ETH value. */
[[nodiscard]] static inline bool
app_format_wei_as_eth(const char *wei_hex, char *out, size_t out_capacity) {
  if (wei_hex == nullptr || out == nullptr ||
      out_capacity < APP_ETH_AMOUNT_CAPACITY || wei_hex[0] != '0' ||
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

  constexpr size_t WEI_DECIMAL_PLACES = 18;
  size_t output_length = 0;
  size_t decimal_point = 0;
  if (decimal_length <= WEI_DECIMAL_PLACES) {
    const size_t leading_zeroes = WEI_DECIMAL_PLACES - decimal_length;
    out[output_length++] = '0';
    decimal_point = output_length;
    out[output_length++] = '.';
    memset(out + output_length, '0', leading_zeroes);
    output_length += leading_zeroes;
    memcpy(out + output_length, decimal, decimal_length);
    output_length += decimal_length;
  } else {
    const size_t integer_length = decimal_length - WEI_DECIMAL_PLACES;
    memcpy(out, decimal, integer_length);
    output_length = integer_length;
    decimal_point = output_length;
    out[output_length++] = '.';
    memcpy(out + output_length, decimal + integer_length, WEI_DECIMAL_PLACES);
    output_length += WEI_DECIMAL_PLACES;
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
