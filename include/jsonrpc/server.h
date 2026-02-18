#pragma once

#include <jsonrpc/jsonrpc.h>
#include <stdint.h>

void server_set_callbacks(jsonrpc_callbacks_t callbacks);
[[nodiscard]] jsonrpc_callbacks_t server_get_callbacks();
void start_jsonrpc_server(const char *host, int32_t port, int32_t backlog,
                          jsonrpc_callbacks_t callbacks);
void server_request_shutdown();
