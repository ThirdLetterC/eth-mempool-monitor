#include "websocket-client/rabbitmq_tx_console_internal.h"
#include "parson/parson.h"
#include "toml/toml.h"
#include "ulog/ulog.h"
#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#if defined(USE_MIMALLOC)
#include <mimalloc.h>
#endif

/*
 * RabbitMQ console process entry point.
 *
 * Signals and command-line arguments are external process inputs. Allocator
 * hooks are configured before parsing so TOML and JSON allocations use the
 * same allocator as the rest of the process.
 */

volatile sig_atomic_t app_shutdown_signal = 0;

static void app_handle_shutdown_signal(int signal_number) {
  app_shutdown_signal = signal_number;
}

static void app_configure_allocator_overrides() {
#if defined(USE_MIMALLOC)
  toml_option_t toml_options = toml_default_option();
  toml_options.mem_realloc = mi_realloc;
  toml_options.mem_free = mi_free;
  toml_set_option(toml_options);
  json_set_allocation_functions(mi_malloc, mi_free);
#endif
}

[[nodiscard]] bool app_is_shutdown_requested() {
  return app_shutdown_signal != 0;
}

[[nodiscard]] static const char *app_signal_name(int signal_number) {
  switch (signal_number) {
  case SIGINT:
    return "SIGINT";
  case SIGTERM:
    return "SIGTERM";
  default:
    return "signal";
  }
}

[[nodiscard]] static bool app_install_signal_handlers() {
  struct sigaction action = {0};
  action.sa_handler = app_handle_shutdown_signal;
  if (sigemptyset(&action.sa_mask) != 0) {
    ulog_error("Failed to initialize signal handler mask\n");
    return false;
  }

  if (sigaction(SIGINT, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGINT handler: %s\n", strerror(errno));
    return false;
  }
  if (sigaction(SIGTERM, &action, nullptr) != 0) {
    ulog_error("Failed to register SIGTERM handler: %s\n", strerror(errno));
    return false;
  }

  return true;
}

int main(int argc, char *argv[]) {
  app_configure_allocator_overrides();

  if (!app_apply_log_style_defaults()) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  app_cli_overrides_t overrides = {0};
  if (!app_parse_cli(argc, argv, &overrides)) {
    app_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    app_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  app_config_t config = {0};
  app_config_set_defaults(&config);

  bool ok = app_load_toml_config(&config, &overrides);
  ok = ok && app_apply_cli_overrides(&config, &overrides);
  if (!ok) {
    app_config_cleanup(&config);
    return EXIT_FAILURE;
  }
  if (!app_apply_log_color(config.log_color)) {
    app_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  if (!app_apply_log_level(config.log_level)) {
    app_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  if (!app_install_signal_handlers()) {
    app_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  ulog_info("Starting RabbitMQ transaction console");
  ulog_info("RabbitMQ endpoint: %s:%u vhost=%s queue=%s durable=%s channel=%u "
            "heartbeat=%u sec",
            config.rabbitmq_host, config.rabbitmq_port, config.rabbitmq_vhost,
            config.rabbitmq_queue,
            config.rabbitmq_queue_durable ? "true" : "false",
            (unsigned)config.rabbitmq_channel,
            (unsigned)config.rabbitmq_heartbeat_seconds);
  ulog_info("Consumer settings: timeout=%u sec prefetch=%u auto_ack=%s",
            (unsigned)config.read_timeout_seconds,
            (unsigned)config.prefetch_count,
            config.auto_ack ? "true" : "false");

  app_rabbitmq_consumer_t consumer = {0};
  if (!app_rabbitmq_connect(&config, &consumer)) {
    app_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  ok = app_consume_loop(&consumer, &config);

  app_rabbitmq_disconnect(&consumer);
  app_config_cleanup(&config);

  if (app_is_shutdown_requested()) {
    ulog_info("Received %s, shutting down",
              app_signal_name((int)app_shutdown_signal));
    return EXIT_SUCCESS;
  }

  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
