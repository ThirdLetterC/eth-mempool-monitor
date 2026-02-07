"""
Example script demonstrating how to load Ethereum addresses from a file
and add them to the monitoring set using the RPC client.

Usage:
    python3 load_addresses_example.py <addresses_file> [host] [port] [auth_token]

Example:
    python3 load_addresses_example.py addresses.txt
    python3 load_addresses_example.py addresses.txt 127.0.0.1 8080 my-secret-token
"""
import sys
import os
from rpc_client import RPCClient, RPCError


def _extract_count(count_result):
    """
    Normalize monitor_count() response into an integer count.

    Supports both legacy scalar responses and object responses like:
    {"set_key": "...", "count": N}
    """
    if isinstance(count_result, dict):
        if 'count' not in count_result:
            raise ValueError(f"monitor_count response missing 'count': {count_result}")
        try:
            return int(count_result['count'])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'count' value in monitor_count response: {count_result}") from exc

    try:
        return int(count_result)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid monitor_count response: {count_result}") from exc


def load_and_monitor_addresses(filepath, host='127.0.0.1', port=8080, auth_token=None):
    """
    Load addresses from a file and add them to the monitoring set.
    
    Args:
        filepath: Path to the file containing addresses (one per line)
        host: RPC server host
        port: RPC server port
        auth_token: Authentication token
    
    Returns:
        True if successful, False otherwise
    """
    if not auth_token:
        auth_token = os.getenv('RPC_CONTROL_AUTH_TOKEN')
    
    if not auth_token:
        print("Error: Authentication token required", file=sys.stderr)
        print("Set RPC_CONTROL_AUTH_TOKEN environment variable or pass as argument", file=sys.stderr)
        return False
    
    try:
        with RPCClient(host=host, port=port, auth_token=auth_token) as client:
            print(f"Connected to RPC server at {host}:{port}")
            
            # Get current count before loading
            count_before_result = client.monitor_count()
            print(f"Monitored addresses before: {count_before_result}")
            count_before = _extract_count(count_before_result)
            
            # Load addresses from file
            print(f"\nLoading addresses from: {filepath}")
            result = client.load_addresses_from_file(filepath)
            
            # Display results
            print("\nResults:")
            if 'added' in result:
                print(f"  Added: {len(result.get('added', []))} addresses")
            if 'already_present' in result:
                print(f"  Already present: {len(result.get('already_present', []))} addresses")
            if 'invalid' in result:
                invalid = result.get('invalid', [])
                if invalid:
                    print(f"  Invalid: {len(invalid)} addresses")
                    print(f"    {invalid}")
            
            # Get count after loading
            count_after_result = client.monitor_count()
            print(f"\nMonitored addresses after: {count_after_result}")
            count_after = _extract_count(count_after_result)
            net_change = count_after - count_before
            print(f"Net change: {net_change:+d}")
            
            return True
            
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return False
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return False
    except ConnectionError as e:
        print(f"Error: Cannot connect to RPC server: {e}", file=sys.stderr)
        print("\nMake sure the rpc_control server is running:", file=sys.stderr)
        print(f"  zig build run-rpc-control -- --config config.toml", file=sys.stderr)
        return False
    except RPCError as e:
        print(f"Error: RPC operation failed: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error: Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    filepath = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else '127.0.0.1'
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8080
    auth_token = sys.argv[4] if len(sys.argv) > 4 else None
    
    success = load_and_monitor_addresses(filepath, host, port, auth_token)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
