#include "websocket-client/http_transmitter_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void test_defaults_and_cli_precedence() {
  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  assert(config.prefetch_count.value == 1);
  assert(config.max_attempts == 3);
  assert(config.connect_timeout.value == 5'000);
  assert(config.parallel_requests.value == 1);
  assert(config.compression == HTTP_COMPRESSION_NONE);

  char program[] = "http_transmitter_config_test";
  char url_option[] = "--webhook-url";
  char url[] = "https://example.invalid/webhook";
  char attempts_option[] = "--webhook-max-attempts";
  char attempts[] = "7";
  char prefetch_option[] = "--prefetch-count";
  char prefetch[] = "4";
  char parallel_option[] = "--webhook-parallel-requests";
  char parallel[] = "4";
  char compression_option[] = "--webhook-compression";
  char compression[] = "zstd";
  char *argv[] = {program,  url_option,         url,        attempts_option,
                  attempts, prefetch_option,    prefetch,   parallel_option,
                  parallel, compression_option, compression};
  http_transmitter_cli_overrides_t overrides = {0};
  assert(http_transmitter_parse_cli((int)(sizeof(argv) / sizeof(argv[0])), argv,
                                    &overrides) == APP_CONFIG_STATUS_OK);
  assert(http_transmitter_apply_cli_overrides(&config, &overrides) ==
         APP_CONFIG_STATUS_OK);
  assert(strcmp(config.webhook_url, url) == 0);
  assert(config.max_attempts == 7);
  assert(config.prefetch_count.value == 4);
  assert(config.parallel_requests.value == 4);
  assert(config.compression == HTTP_COMPRESSION_ZSTD);
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

static void test_parallel_requests_toml() {
  constexpr char TOML[] = "[rabbitmq_consumer]\n"
                          "prefetch_count = 6\n"
                          "[webhook]\n"
                          "url = \"https://example.invalid/webhook\"\n"
                          "parallel_requests = 6\n"
                          "compression = \"brotli\"\n";
  char path[] = "/tmp/http-transmitter-config-XXXXXX";
  int descriptor = mkstemp(path);
  assert(descriptor >= 0);
  assert(write(descriptor, TOML, sizeof(TOML) - 1) ==
         (ssize_t)(sizeof(TOML) - 1));
  assert(close(descriptor) == 0);

  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  http_transmitter_cli_overrides_t overrides = {
      .config_path = path,
      .config_path_set = true,
  };
  assert(http_transmitter_load_toml_config(&config, &overrides) ==
         APP_CONFIG_STATUS_OK);
  assert(config.prefetch_count.value == 6);
  assert(config.parallel_requests.value == 6);
  assert(config.compression == HTTP_COMPRESSION_BROTLI);
  assert(http_transmitter_finalize_config(&config) == APP_CONFIG_STATUS_OK);
  http_transmitter_config_cleanup(&config);
  assert(unlink(path) == 0);
}

static void test_invalid_configuration() {
  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  config.webhook_url = "file:///tmp/not-allowed";
  assert(http_transmitter_finalize_config(&config) ==
         APP_CONFIG_STATUS_INVALID_VALUE);
  http_transmitter_config_cleanup(&config);

  http_transmitter_config_set_defaults(&config);
  config.webhook_url = "https://example.invalid/webhook";
  config.parallel_requests.value = 2;
  assert(http_transmitter_finalize_config(&config) ==
         APP_CONFIG_STATUS_INVALID_VALUE);
  http_transmitter_config_cleanup(&config);
}

int main() {
  test_defaults_and_cli_precedence();
  test_bearer_token_environment();
  test_parallel_requests_toml();
  test_invalid_configuration();
  return EXIT_SUCCESS;
}
