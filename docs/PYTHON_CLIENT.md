# Python RPC Client Usage Guide

This guide explains how to use the Python RPC client to interact with the `rpc_control` server.

## Quick Start

### 1. Start the Required Services

```bash
# Start Redis and RabbitMQ (if needed)
docker compose -f compose.yml up -d

# Build the project
zig build

# Start the RPC control server
# Set rpc_control.auth_token in config.toml first.
zig build run-rpc-control -- --config config.toml
```

### 2. Use the Client

#### Interactive Demo

Run the built-in demo to see all features:

```bash
python3 rpc_client.py 127.0.0.1 8080 your-token
```

## Usage in Your Code

### Basic Connection

```python
from rpc_client import RPCClient

# Simple connection
client = RPCClient(host='127.0.0.1', port=8080, auth_token='your-token')
# ... use client ...
client.close()

# Better: Use context manager for automatic cleanup
with RPCClient(host='127.0.0.1', port=8080, auth_token='your-token') as client:
    # Client automatically closes when exiting the block
    result = client.ping()
```

### Adding Addresses

```python
with RPCClient(auth_token='your-token') as client:
    # Add a single address
    client.monitor_add('0x1111111111111111111111111111111111111111')
    
    # Add multiple addresses at once
    client.monitor_add([
        '0x2222222222222222222222222222222222222222',
        '0x3333333333333333333333333333333333333333'
    ])
    
    # Load addresses from a file
    client.load_addresses_from_file('addresses.txt')
    
    # Alternative method names (aliases)
    client.add_address('0x4444444444444444444444444444444444444444')
    client.add_addresses(['0x5555...', '0x6666...'])
```

#### Address File Format

When using `load_addresses_from_file()`, the file should contain one address per line:

```
# This is a comment (lines starting with # are ignored)
0x1111111111111111111111111111111111111111
0x2222222222222222222222222222222222222222

# Empty lines are also ignored
0x3333333333333333333333333333333333333333
  0x4444444444444444444444444444444444444444  # Whitespace is trimmed
```

### Checking Addresses

```python
with RPCClient(auth_token='your-token') as client:
    # Check if a single address is monitored
    is_monitored = client.monitor_has('0x1111111111111111111111111111111111111111')
    print(f"Monitored: {is_monitored}")  # True or False
    
    # Check multiple addresses
    results = client.monitor_has([
        '0x1111111111111111111111111111111111111111',
        '0x2222222222222222222222222222222222222222'
    ])
    # Returns: {'0x1111...': True, '0x2222...': False}
    
    # Alternative method name
    is_monitored = client.is_monitored('0x1111...')
```

### Listing Addresses

```python
with RPCClient(auth_token='your-token') as client:
    # Get count of monitored addresses
    count = client.monitor_count()
    print(f"Total monitored: {count}")
    
    # Get all monitored addresses
    addresses = client.monitor_list()
    for addr in addresses:
        print(f"  - {addr}")
```

### Removing Addresses

```python
with RPCClient(auth_token='your-token') as client:
    # Remove a single address
    client.monitor_remove('0x1111111111111111111111111111111111111111')
    
    # Remove multiple addresses
    client.monitor_remove([
        '0x2222222222222222222222222222222222222222',
        '0x3333333333333333333333333333333333333333'
    ])
    
    # Alternative method names
    client.remove_address('0x4444...')
    client.remove_addresses(['0x5555...', '0x6666...'])
```

### Clearing All Addresses

```python
with RPCClient(auth_token='your-token') as client:
    # Must explicitly confirm to prevent accidental clearing
    client.monitor_clear(confirm=True)
```

### Health Checks

```python
with RPCClient(auth_token='your-token') as client:
    # Simple ping
    pong = client.ping()
    
    # Get health status
    health = client.health()
    
    # List available methods
    methods = client.methods()
    print(f"Available: {', '.join(methods)}")
```

## Error Handling

### Connection Errors

```python
from rpc_client import RPCClient, RPCError

try:
    client = RPCClient(host='127.0.0.1', port=8080, auth_token='your-token')
except ConnectionError as e:
    print(f"Cannot connect to server: {e}")
    print("Make sure rpc_control is running!")
```

### RPC Errors

```python
from rpc_client import RPCClient, RPCError

try:
    with RPCClient(auth_token='your-token') as client:
        # Some operation that might fail
        result = client.monitor_add('invalid-address')
except RPCError as e:
    print(f"RPC Error {e.code}: {e.message}")
    if e.data:
        print(f"Additional info: {e.data}")
```

### Validation Errors

```python
from rpc_client import RPCClient

client = RPCClient(auth_token='your-token')

# This will raise ValueError
try:
    client.monitor_clear(confirm=False)
except ValueError as e:
    print(f"Validation error: {e}")
```

## Advanced Usage

### Custom Timeout

```python
# Set a custom timeout (in seconds)
client = RPCClient(host='127.0.0.1', port=8080, timeout=60.0, auth_token='your-token')
```

### Multiple Clients

```python
# You can have multiple client instances
with RPCClient(host='server1.example.com', port=8080, auth_token='token-1') as client1, \
     RPCClient(host='server2.example.com', port=8080, auth_token='token-2') as client2:
    
    count1 = client1.monitor_count()
    count2 = client2.monitor_count()
    print(f"Server 1: {count1}, Server 2: {count2}")
```

### Batch Operations

```python
with RPCClient(auth_token='your-token') as client:
    # Prepare a list of addresses
    addresses = [f'0x{i:040x}' for i in range(1, 11)]  # 10 addresses
    
    # Add them all at once
    client.monitor_add(addresses)
    
    # Verify they were all added
    count = client.monitor_count()
    print(f"Added {len(addresses)} addresses, count is now {count}")
    
    # Remove them all at once
    client.monitor_remove(addresses)
```

## Integration Example

Here's a complete example of monitoring Ethereum addresses loaded from a file:

```python
#!/usr/bin/env python3
from rpc_client import RPCClient, RPCError
import sys

def setup_monitoring(addresses_file):
    """Load addresses from file and add to monitoring."""
    # Connect and add addresses using the built-in method
    try:
        with RPCClient(auth_token='your-token') as client:
            print(f"Loading addresses from {addresses_file}...")
            result = client.load_addresses_from_file(addresses_file)
            
            # Display results
            print(f"Successfully loaded addresses:")
            if 'added' in result:
                print(f"  Added: {len(result['added'])} new addresses")
            if 'already_present' in result:
                print(f"  Already present: {len(result['already_present'])} addresses")
            
            # Verify
            count = client.monitor_count()
            print(f"Total monitored addresses: {count}")
            
            return True
    except ConnectionError as e:
        print(f"Error: Cannot connect to RPC server: {e}", file=sys.stderr)
        return False
    except RPCError as e:
        print(f"Error: RPC operation failed: {e}", file=sys.stderr)
        return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: script.py <addresses_file>")
        sys.exit(1)
    
    success = setup_monitoring(sys.argv[1])
    sys.exit(0 if success else 1)
```

## Troubleshooting

### Server Not Running

If you get `Connection refused` or `Failed to connect`, make sure:

1. Redis/Valkey is running: `docker compose ps`
2. The RPC control server is running: `zig build run-rpc-control -- --config config.toml`
3. The server is listening on the correct port (default: 8080)

### Timeout Errors

If operations timeout:

1. Check if Redis is responsive
2. Increase the timeout: `RPCClient(timeout=60.0, auth_token='your-token')`
3. Check network connectivity

### Invalid Addresses

The server may validate Ethereum address format. Ensure addresses:

- Start with `0x`
- Are 42 characters long (0x + 40 hex digits)
- Are lowercase or checksum format

## API Reference

### RPCClient Methods

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `ping()` | - | str | Test server connectivity |
| `authenticate(token)` | str | dict | Authenticate this TCP connection |
| `health()` | - | dict | Get server health status |
| `methods()` | - | list[str] | Get available RPC methods |
| `monitor_add(address)` | str or list[str] | dict | Add address(es) to monitoring |
| `load_addresses_from_file(filepath)` | str | dict | Load addresses from file and add to monitoring |
| `monitor_remove(address)` | str or list[str] | dict | Remove address(es) from monitoring |
| `monitor_has(address)` | str or list[str] | bool or dict | Check if address(es) monitored |
| `monitor_count()` | - | int | Get count of monitored addresses |
| `monitor_list()` | - | list[str] | Get all monitored addresses |
| `monitor_clear(confirm)` | bool | dict | Clear all (requires confirm=True) |
| `close()` | - | None | Close connection |

### Method Aliases

- `add_address()` → `monitor_add()`
- `add_addresses()` → `monitor_add()`
- `remove_address()` → `monitor_remove()`
- `remove_addresses()` → `monitor_remove()`
- `is_monitored()` → `monitor_has()`

## Requirements

- Python 3.6+ (uses type hints)
- No external dependencies (standard library only)
- Running `rpc_control` server
- Redis/Valkey instance (for the server)
