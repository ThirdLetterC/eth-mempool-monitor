#include "websocket-client/http_transmitter_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

static void test_defaults_and_cli_precedence() {
  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  assert(config.prefetch_count.value == 1);
  assert(config.max_attempts == 3);
  assert(config.connect_timeout.value == 5'000);

  char program[] = "http_transmitter_config_test";
  char url_option[] = "--webhook-url";
  char url[] = "https://example.invalid/webhook";
  char attempts_option[] = "--webhook-max-attempts";
  char attempts[] = "7";
  char prefetch_option[] = "--prefetch-count";
  char prefetch[] = "4";
  char *argv[] = {program,  url_option,      url,     attempts_option,
                  attempts, prefetch_option, prefetch};
  http_transmitter_cli_overrides_t overrides = {0};
  assert(http_transmitter_parse_cli((int)(sizeof(argv) / sizeof(argv[0])), argv,
                                    &overrides) == APP_CONFIG_STATUS_OK);
  assert(http_transmitter_apply_cli_overrides(&config, &overrides) ==
         APP_CONFIG_STATUS_OK);
  assert(strcmp(config.webhook_url, url) == 0);
  assert(config.max_attempts == 7);
  assert(config.prefetch_count.value == 4);
  assert(http_transmitter_finalize_config(&config) == APP_CONFIG_STATUS_OK);
  http_transmitter_config_cleanup(&config);
}

static void test_bearer_token_environment() {
  constexpr char ENVIRONMENT_NAME[] = "HTTP_TRANSMITTER_TEST_TOKEN";
  constexpr char TOKEN[] = "test-secret";
  assert(setenv(ENVIRONMENT_NAME, TOKEN, 1) == 0);

  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  char program[] = "http_transmitter_config_test";
  char url_option[] = "--webhook-url";
  char url[] = "http://127.0.0.1:8080/hook";
  char token_option[] = "--webhook-bearer-token-env";
  char token_environment[] = "HTTP_TRANSMITTER_TEST_TOKEN";
  char *argv[] = {program, url_option, url, token_option, token_environment};
  http_transmitter_cli_overrides_t overrides = {0};
  assert(http_transmitter_parse_cli((int)(sizeof(argv) / sizeof(argv[0])), argv,
                                    &overrides) == APP_CONFIG_STATUS_OK);
  assert(http_transmitter_apply_cli_overrides(&config, &overrides) ==
         APP_CONFIG_STATUS_OK);
  assert(http_transmitter_finalize_config(&config) == APP_CONFIG_STATUS_OK);
  assert(config.bearer_token != nullptr);
  assert(strcmp(config.bearer_token, TOKEN) == 0);
  http_transmitter_config_cleanup(&config);
  assert(unsetenv(ENVIRONMENT_NAME) == 0);
}

static void test_invalid_configuration() {
  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  config.webhook_url = "file:///tmp/not-allowed";
  assert(http_transmitter_finalize_config(&config) ==
         APP_CONFIG_STATUS_INVALID_VALUE);
  http_transmitter_config_cleanup(&config);
}

int main() {
  test_defaults_and_cli_precedence();
  test_bearer_token_environment();
  test_invalid_configuration();
  return EXIT_SUCCESS;
}
