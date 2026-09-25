#include "websocket-client/monitor_config.h"
#include "websocket-client/monitor_runtime.h"

#include "ulog/ulog.h"

#include <stdlib.h>

#if defined(USE_MIMALLOC)
#include "hiredis/alloc.h"
#include <mimalloc.h>
#include <uv.h>
#include <wolfssl/options.h>
#include <wolfssl/wolfcrypt/memory.h>

#include "parson/parson.h"
#include "toml/toml.h"
#endif

/*
 * Monitor process entry point.
 *
 * Allocator hooks must be installed before configuration parsing because TOML
 * and JSON values can outlive the call that created them. The single cleanup
 * path then releases all configuration-owned strings with the same allocator.
 */
[[nodiscard]] static bool app_configure_allocator_overrides() {
#if defined(USE_MIMALLOC)
  toml_option_t toml_options = toml_default_option();
  toml_options.mem_realloc = mi_realloc;
  toml_options.mem_free = mi_free;
  toml_set_option(toml_options);
  json_set_allocation_functions(mi_malloc, mi_free);

  hiredisAllocFuncs hiredis_allocators = {
      .mallocFn = mi_malloc,
      .callocFn = mi_calloc,
      .reallocFn = mi_realloc,
      .strdupFn = mi_strdup,
      .freeFn = mi_free,
  };
  (void)hiredisSetAllocators(&hiredis_allocators);

  if (uv_replace_allocator(mi_malloc, mi_realloc, mi_calloc, mi_free) != 0 ||
      wolfSSL_SetAllocators(mi_malloc, mi_free, mi_realloc) != 0) {
    return false;
  }
#endif
  return true;
}

int main(int argc, char *argv[]) {
  if (!app_configure_allocator_overrides()) {
    return EXIT_FAILURE;
  }

  /* Configuration owns any strings copied from TOML or command-line input. */
  int exit_code = EXIT_FAILURE;
  monitor_config_t config = {0};
  monitor_cli_overrides_t overrides = {0};

  if (!app_apply_log_style_defaults()) {
    goto cleanup;
  }

  if (app_parse_cli(argc, argv, &overrides) != APP_CONFIG_STATUS_OK) {
    app_print_usage(argv[0]);
    goto cleanup;
  }
  if (overrides.show_help) {
    app_print_usage(argv[0]);
    exit_code = EXIT_SUCCESS;
    goto cleanup;
  }

  app_config_set_defaults(&config);
  if (app_load_toml_config(&config, &overrides) != APP_CONFIG_STATUS_OK ||
      app_apply_cli_overrides(&config, &overrides) != APP_CONFIG_STATUS_OK ||
      !app_apply_log_color(config.log_color) ||
      !app_apply_log_level(config.log_level)) {
    goto cleanup;
  }

  if (app_runtime_run(&config) == MONITOR_RUNTIME_STATUS_OK) {
    exit_code = EXIT_SUCCESS;
  }

cleanup:
  app_config_cleanup(&config);
  (void)ulog_cleanup();
  return exit_code;
}
