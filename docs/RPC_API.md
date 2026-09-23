# `rpc_control` JSON-RPC API

`rpc_control` uses newline-delimited JSON-RPC 2.0 over raw TCP. It binds to
`127.0.0.1` by default. Every method except `ping` requires the connection to
authenticate first with `auth`.

## Methods

- `ping`
- `auth` with `{"token":"<rpc_control.auth_token>"}`
- `health`
- `methods`
- `monitor_add` (aliases: `add_address`, `add_addresses`)
- `monitor_remove` (aliases: `remove_address`, `remove_addresses`)
- `monitor_has` (alias: `is_monitored`)
- `monitor_count`
- `monitor_list`
- `monitor_clear` with `{"confirm":true}`

`monitor_add`, `monitor_remove`, and `monitor_has` accept:

- a single address string;
- an array of address strings; or
- an object containing `address` or `addresses`.

## Examples

Authenticate:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"auth","params":{"token":"<rpc_control.auth_token>"}}' \
  | nc 127.0.0.1 8080
```

Add and list monitored addresses:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"monitor_add","params":{"address":"0x1111111111111111111111111111111111111111"}}' \
  | nc 127.0.0.1 8080
printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"monitor_list"}' \
  | nc 127.0.0.1 8080
```

Check an address:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":4,"method":"monitor_has","params":{"address":"0x1111111111111111111111111111111111111111"}}' \
  | nc 127.0.0.1 8080
```

See [PYTHON_CLIENT.md](PYTHON_CLIENT.md) for a higher-level client interface and
[CONFIG.md](CONFIG.md) for server configuration keys.
