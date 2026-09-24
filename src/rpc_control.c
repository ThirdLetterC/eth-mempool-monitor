#include "jsonrpc/server.h"
#include "rpc_control/config_internal.h"
#include "rpc_control/service_internal.h"
#include "ulog/ulog.h"

#include <inttypes.h>
#include <signal.h>
#include <stdlib.h>
#include <uv.h>

/*
 * JSON-RPC control process entry point.
 *
 * Configuration, Redis state, and the libuv server are initialized in that
 * order. Shutdown reverses the owned-resource portion of that sequence.
 */
static volatile sig_atomic_t g_shutdown_signal = 0;

static void rpc_control_on_signal(uv_signal_t *handle, int signum) {
  /* libuv invokes this callback on its event-loop thread, so requesting an
   * orderly server shutdown is safe here. */
  g_shutdown_signal = signum;
  (void)uv_signal_stop(handle);
  uv_close((uv_handle_t *)handle, nullptr);
  server_request_shutdown();
}

int main(int argc, char **argv) {
  rpc_control_configure_allocator_overrides();

  if (!rpc_control_apply_log_style_defaults()) {
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  rpc_control_cli_overrides_t overrides = {0};
  if (!rpc_control_parse_cli(argc, argv, &overrides)) {
    rpc_control_print_usage(argv[0]);
    return EXIT_FAILURE;
  }
  if (overrides.show_help) {
    rpc_control_print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  rpc_control_config_t config = {0};
  rpc_control_config_set_defaults(&config);

  bool config_ok = rpc_control_load_toml_config(&config, &overrides);
  config_ok = config_ok && rpc_control_apply_cli_overrides(&config, &overrides);
  if (!config_ok) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }
  if (!rpc_control_apply_log_color(config.log_color)) {
    rpc_control_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }
  if (!rpc_control_apply_log_level(config.log_level)) {
    rpc_control_config_cleanup(&config);
    (void)ulog_cleanup();
    return EXIT_FAILURE;
  }

  if (!rpc_control_validate_security_config(&config)) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  if (!rpc_control_connect_redis(&config)) {
    rpc_control_config_cleanup(&config);
    return EXIT_FAILURE;
  }

  ulog_info("Starting RPC control server on %s:%" PRId32 " (backlog=%" PRId32
            ")",
            config.host, config.port, config.backlog);
  ulog_info("RPC control authentication enabled (token required via 'auth' "
            "method)");

  jsonrpc_callbacks_t callbacks = {
      .on_open = rpc_control_on_open,
      .on_close = rpc_control_on_close,
      .on_request = rpc_control_on_request,
      .on_notification = rpc_control_on_notification,
  };

  uv_loop_t *loop = uv_default_loop();
  uv_signal_t sigint_handle = {0};
  uv_signal_t sigterm_handle = {0};

  if (loop != nullptr && uv_signal_init(loop, &sigint_handle) == 0) {
    (void)uv_signal_start(&sigint_handle, rpc_control_on_signal, SIGINT);
  }
  if (loop != nullptr && uv_signal_init(loop, &sigterm_handle) == 0) {
    (void)uv_signal_start(&sigterm_handle, rpc_control_on_signal, SIGTERM);
  }

  start_jsonrpc_server(config.host, config.port, config.backlog, callbacks);
  rpc_control_disconnect_redis();
  rpc_control_config_cleanup(&config);

  if (g_shutdown_signal != 0) {
    ulog_info("Shutdown signal received (%d)", (int)g_shutdown_signal);
  }

  return EXIT_SUCCESS;
}
