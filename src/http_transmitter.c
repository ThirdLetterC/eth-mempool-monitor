#include "websocket-client/http_transmitter_internal.h"
#include "ulog/ulog.h"

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#if defined(USE_MIMALLOC)
#include "parson/parson.h"
#include "toml/toml.h"
#include <mimalloc.h>
#include <wolfssl/options.h>
#include <wolfssl/wolfcrypt/memory.h>
#endif

volatile sig_atomic_t http_transmitter_shutdown_signal = 0;

static void http_transmitter_handle_signal(int signal_number) {
  http_transmitter_shutdown_signal = signal_number;
}

[[nodiscard]] bool http_transmitter_is_shutdown_requested() {
  return http_transmitter_shutdown_signal != 0;
}

[[nodiscard]] static bool http_transmitter_configure_allocators() {
#if defined(USE_MIMALLOC)
  toml_option_t options = toml_default_option();
  options.mem_realloc = mi_realloc;
  options.mem_free = mi_free;
  toml_set_option(options);
  json_set_allocation_functions(mi_malloc, mi_free);
  if (wolfSSL_SetAllocators(mi_malloc, mi_free, mi_realloc) != 0) {
    return false;
  }
#endif
  return true;
}

[[nodiscard]] static bool http_transmitter_install_signal_handlers() {
  struct sigaction action = {0};
  action.sa_handler = http_transmitter_handle_signal;
  if (sigemptyset(&action.sa_mask) != 0 ||
      sigaction(SIGINT, &action, nullptr) != 0 ||
      sigaction(SIGTERM, &action, nullptr) != 0) {
    ulog_error("Failed to install shutdown signal handlers: %s",
               strerror(errno));
    return false;
  }
  return true;
}

int main(int argc, char *argv[]) {
  if (!http_transmitter_configure_allocators()) {
    return EXIT_FAILURE;
  }
  if (!http_transmitter_apply_log_style_defaults()) {
    return EXIT_FAILURE;
  }

  http_transmitter_cli_overrides_t overrides = {0};
  if (http_transmitter_parse_cli(argc, argv, &overrides) !=
      APP_CONFIG_STATUS_OK) {
    http_transmitter_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    http_transmitter_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  http_transmitter_config_t config = {0};
  http_transmitter_config_set_defaults(&config);
  bool configured = http_transmitter_load_toml_config(&config, &overrides) ==
                    APP_CONFIG_STATUS_OK;
  configured = configured && http_transmitter_apply_cli_overrides(
                                 &config, &overrides) == APP_CONFIG_STATUS_OK;
  configured = configured && http_transmitter_finalize_config(&config) ==
                                 APP_CONFIG_STATUS_OK;
  if (!configured || !http_transmitter_apply_log_color(config.log_color) ||
      !http_transmitter_apply_log_level(config.log_level)) {
    http_transmitter_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  if (!http_transmitter_install_signal_handlers()) {
    http_transmitter_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  http_transmitter_consumer_t consumer = {0};
  if (http_transmitter_rabbitmq_connect(&config, &consumer) !=
      HTTP_TRANSMITTER_STATUS_OK) {
    http_transmitter_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  ulog_info("Starting HTTP transaction transmitter");
  ulog_info("Webhook delivery: parallel=%u attempts=%u compression=%s "
            "connect_timeout=%u ms timeout=%u ms",
            (unsigned)config.parallel_requests.value, config.max_attempts,
            http_transmitter_compression_name(config.compression),
            config.connect_timeout.value, config.request_timeout.value);
  bool ok = http_transmitter_consume_loop(&consumer, &config) ==
            HTTP_TRANSMITTER_STATUS_OK;

  http_transmitter_rabbitmq_disconnect(&consumer);
  http_transmitter_config_cleanup(&config);
  (void)ulog_cleanup();
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
