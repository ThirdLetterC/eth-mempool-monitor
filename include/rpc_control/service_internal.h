#pragma once

/**
 * Internal rpc_control service boundary.
 * JSON-RPC requests and Redis replies are untrusted; handlers validate types,
 * address bounds, authentication state, and backend result shapes.
 */

#include "jsonrpc/jsonrpc.h"
#include "rpc_control/config_internal.h"

[[nodiscard]] bool
rpc_control_connect_redis(const rpc_control_config_t *config);
void rpc_control_disconnect_redis();

void rpc_control_on_open(jsonrpc_conn_t *conn);
void rpc_control_on_close(jsonrpc_conn_t *conn);
[[nodiscard]] bool rpc_control_on_request(jsonrpc_conn_t *conn,
                                          const char *method,
                                          const JSON_Value *params,
                                          jsonrpc_response_t *response);
void rpc_control_on_notification(jsonrpc_conn_t *conn, const char *method,
                                 const JSON_Value *params);
