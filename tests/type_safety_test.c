#include "app/domain_types.h"
#include "app/ethereum_quantity.h"
#include "websocket-client/rabbitmq_publisher.h"
#include "websocket-client/subscriber.h"
#include "websocket-client/ws_client.h"

#include <assert.h>
#include <string.h>

static_assert(sizeof(ws_transport_t) == sizeof(uint8_t));
static_assert(sizeof(ws_status_t) == sizeof(uint8_t));
static_assert(sizeof(ws_rabbitmq_status_t) == sizeof(uint8_t));
static_assert(sizeof(ws_subscriber_status_t) == sizeof(uint8_t));
static_assert(sizeof(app_config_status_t) == sizeof(uint8_t));

int main() {
  app_port_t port = {.value = 65'535};
  app_seconds_t seconds = {.value = 30};
  app_milliseconds_t milliseconds = {.value = 30};
  app_rabbitmq_channel_t channel = {.value = 1};
  app_prefetch_count_t prefetch = {.value = 100};
  app_port_t converted_port = {0};
  app_seconds_t converted_seconds = {0};
  app_socket_backlog_t converted_backlog = {0};
  char eth_amount[APP_ETH_AMOUNT_CAPACITY] = {0};

  assert(port.value == UINT16_MAX);
  assert(seconds.value == 30);
  assert(milliseconds.value == 30);
  assert(channel.value == 1);
  assert(prefetch.value == 100);
  assert(app_port_from_u64(443, &converted_port));
  assert(converted_port.value == 443);
  assert(!app_port_from_u64(0, &converted_port));
  assert(!app_port_from_u64((uint64_t)UINT16_MAX + 1, &converted_port));
  assert(app_seconds_from_u64(UINT32_MAX, &converted_seconds));
  assert(!app_seconds_from_u64((uint64_t)UINT32_MAX + 1, &converted_seconds));
  assert(app_socket_backlog_from_u64(INT32_MAX, &converted_backlog));
  assert(!app_socket_backlog_from_u64((uint64_t)INT32_MAX + 1,
                                      &converted_backlog));
  assert(WS_TRANSPORT_PLAIN != WS_TRANSPORT_TLS);
  assert(WS_STATUS_OK != WS_STATUS_PROTOCOL_ERROR);
  assert(strcmp(ws_status_string(WS_STATUS_BUFFER_TOO_SMALL),
                "buffer too small") == 0);
  assert(WS_RABBITMQ_STATUS_OK != WS_RABBITMQ_STATUS_QUEUE_FULL);
  assert(WS_SUBSCRIBER_STATUS_STOPPED !=
         WS_SUBSCRIBER_STATUS_RECONNECT_REQUIRED);
  assert(APP_CONFIG_STATUS_OK != APP_CONFIG_STATUS_INVALID_VALUE);
  assert(app_format_wei_as_eth("0x0", eth_amount, sizeof(eth_amount)));
  assert(strcmp(eth_amount, "0") == 0);
  assert(app_format_wei_as_eth("0x1", eth_amount, sizeof(eth_amount)));
  assert(strcmp(eth_amount, "0.000000000000000001") == 0);
  assert(app_format_wei_as_eth("0xde0b6b3a7640000", eth_amount,
                               sizeof(eth_amount)));
  assert(strcmp(eth_amount, "1") == 0);
  assert(app_format_wei_as_eth("0x14d1120d7b160000", eth_amount,
                               sizeof(eth_amount)));
  assert(strcmp(eth_amount, "1.5") == 0);
  assert(app_format_wei_as_eth(
      "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
      eth_amount, sizeof(eth_amount)));
  assert(strcmp(eth_amount,
                "115792089237316195423570985008687907853269984665640564039457."
                "584007913129639935") == 0);
  assert(!app_format_wei_as_eth("0xnot-hex", eth_amount, sizeof(eth_amount)));
  return 0;
}
