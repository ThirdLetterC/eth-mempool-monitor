#pragma once

/**
 * Internal rpc_control configuration boundary.
 * CLI arguments and TOML values are untrusted; this module owns their parsing,
 * validation, copied strings, and cleanup.
 */

#include "ulog/ulog.h"

#include <stdint.h>

typedef struct rpc_control_config rpc_control_config_t;
struct rpc_control_config {
  const char *host;
  int32_t port;
  int32_t backlog;
  const char *auth_token;
  ulog_level log_level;
  bool log_color;
  const char *redis_host;
  uint16_t redis_port;
  const char *redis_set_key;
  char *host_owned;
  char *auth_token_owned;
  char *redis_host_owned;
  char *redis_set_key_owned;
};

typedef struct rpc_control_cli_overrides rpc_control_cli_overrides_t;
struct rpc_control_cli_overrides {
  const char *config_path;
  bool config_path_set;
  const char *host;
  const char *port_text;
  const char *backlog_text;
  const char *auth_token;
  const char *redis_host;
  const char *redis_port_text;
  const char *redis_set_key;
  bool show_help;
};

void rpc_control_configure_allocator_overrides();
void rpc_control_print_usage(const char *program_name);
void rpc_control_config_set_defaults(rpc_control_config_t *config);
void rpc_control_config_cleanup(rpc_control_config_t *config);
[[nodiscard]] bool rpc_control_apply_log_level(ulog_level level);
[[nodiscard]] bool rpc_control_apply_log_color(bool enabled);
[[nodiscard]] bool rpc_control_apply_log_style_defaults();
[[nodiscard]] bool
rpc_control_load_toml_config(rpc_control_config_t *config,
                             const rpc_control_cli_overrides_t *overrides);
[[nodiscard]] bool
rpc_control_parse_cli(int argc, char *argv[],
                      rpc_control_cli_overrides_t *overrides);
[[nodiscard]] bool
rpc_control_apply_cli_overrides(rpc_control_config_t *config,
                                const rpc_control_cli_overrides_t *overrides);
[[nodiscard]] bool
rpc_control_validate_security_config(const rpc_control_config_t *config);
