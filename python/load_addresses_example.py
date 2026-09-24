"""
Example script demonstrating how to load Ethereum addresses from a file
and add them to the monitoring set using the RPC client.

Usage:
    python3 python/load_addresses_example.py <addresses_file> [host] [port] [auth_token]

Example:
    python3 python/load_addresses_example.py conf/addresses.toml
    python3 python/load_addresses_example.py conf/addresses.toml 127.0.0.1 8080 my-secret-token
"""

import argparse
import os
import sys
from typing import Any

if __package__:
    from .rpc_client import DEFAULT_ADDRESS_BATCH_SIZE, RPCClient, RPCError
else:
    from rpc_client import DEFAULT_ADDRESS_BATCH_SIZE, RPCClient, RPCError


def _extract_count(count_result: Any) -> int:
    """
    Normalize monitor_count() response into an integer count.

    Supports both legacy scalar responses and object responses like:
    {"set_key": "...", "count": N}
    """
    if isinstance(count_result, dict):
        if "count" not in count_result:
            raise ValueError(f"monitor_count response missing 'count': {count_result}")
        try:
            return int(count_result["count"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Invalid 'count' value in monitor_count response: {count_result}"
            ) from exc

    try:
        return int(count_result)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid monitor_count response: {count_result}") from exc


def load_and_monitor_addresses(
    filepath: str,
    host: str = "127.0.0.1",
    port: int = 8080,
    auth_token: str | None = None,
    batch_size: int = DEFAULT_ADDRESS_BATCH_SIZE,
) -> bool:
    """
    Load addresses from a file and add them to the monitoring set.

    Args:
        filepath: Path to a TOML file containing an ``addresses`` array
        host: RPC server host
        port: RPC server port
        auth_token: Authentication token
        batch_size: Maximum addresses sent in each RPC request

    Returns:
        True if successful, False otherwise
    """
    if not auth_token:
        auth_token = os.getenv("RPC_CONTROL_AUTH_TOKEN")

    if not auth_token:
        print("Error: Authentication token required", file=sys.stderr)
        print(
            "Set RPC_CONTROL_AUTH_TOKEN environment variable or pass as argument", file=sys.stderr
        )
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

            def report_progress(completed_batches: int, processed_addresses: int) -> None:
                if completed_batches == 1 or completed_batches % 100 == 0:
                    print(
                        f"  Imported {processed_addresses:,} addresses "
                        f"in {completed_batches:,} batches..."
                    )

            result = client.load_addresses_from_file_batched(
                filepath,
                batch_size=batch_size,
                progress=report_progress,
            )

            # Display results
            print("\nResults:")
            print(f"  Requested: {result['requested_count']:,} addresses")
            print(f"  Added: {result['added_count']:,} addresses")
            print(f"  Already present: {result['already_present_count']:,} addresses")
            print(f"  Invalid: {result['invalid_count']:,} addresses")
            print(f"  Batches: {result['batch_count']:,}")

            # Get count after loading
            count_after_result = client.monitor_count()
            print(f"\nMonitored addresses after: {count_after_result}")
            count_after = _extract_count(count_after_result)
            net_change = count_after - count_before
            print(f"Net change: {net_change:+d}")

            return True

    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return False
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return False
    except ConnectionError as exc:
        print(f"Error: Cannot connect to RPC server: {exc}", file=sys.stderr)
        print("\nMake sure the rpc_control server is running:", file=sys.stderr)
        print("  zig build run-rpc-control -- --config conf/config.toml", file=sys.stderr)
        return False
    except RPCError as exc:
        print(f"Error: RPC operation failed: {exc}", file=sys.stderr)
        return False


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("filepath", help="TOML file containing an addresses array")
    parser.add_argument("host", nargs="?", default="127.0.0.1", help="RPC server host")
    parser.add_argument("port", nargs="?", type=int, default=8080, help="RPC server port")
    parser.add_argument("auth_token", nargs="?", help="RPC authentication token")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_ADDRESS_BATCH_SIZE,
        help=f"addresses per RPC request (maximum: {DEFAULT_ADDRESS_BATCH_SIZE})",
    )
    args = parser.parse_args()

    success = load_and_monitor_addresses(
        args.filepath,
        args.host,
        args.port,
        args.auth_token,
        args.batch_size,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
