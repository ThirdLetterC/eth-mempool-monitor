"""
RPC Client for eth-mempool-monitor rpc_control server.

This module provides a Python client to interact with the JSON-RPC TCP server
exposed by the rpc_control binary. The server manages monitored Ethereum addresses
stored in Redis.

Usage:
    from python.rpc_client import RPCClient

    # Connect to the RPC server
    client = RPCClient(host='127.0.0.1', port=8080, auth_token='your-token')

    # Add an address to monitor
    result = client.monitor_add('0x1111111111111111111111111111111111111111')

    # Load addresses from a file
    result = client.load_addresses_from_file('conf/addresses.txt')

    # List all monitored addresses
    addresses = client.monitor_list()

    # Check if an address is monitored
    is_monitored = client.monitor_has('0x1111111111111111111111111111111111111111')

    # Remove an address from monitoring
    result = client.monitor_remove('0x1111111111111111111111111111111111111111')

    # Close the connection
    client.close()
"""

import json
import socket
from contextlib import suppress
from math import isfinite
from pathlib import Path
from threading import Lock
from types import TracebackType
from typing import Any, Optional, Union

DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024

AddressInput = Union[str, list[str]]
JsonObject = dict[str, Any]


class RPCError(Exception):
    """Exception raised when the RPC server returns an error."""

    def __init__(self, code: int, message: str, data: Optional[Any] = None) -> None:
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"RPC Error {code}: {message}")


class RPCProtocolError(ValueError):
    """Raised when the server returns an invalid JSON-RPC response."""


class RPCClient:
    """
    JSON-RPC 2.0 client for eth-mempool-monitor rpc_control server.

    The server accepts newline-delimited JSON-RPC requests over TCP and
    manages a set of monitored Ethereum addresses in Redis.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        timeout: float = 30.0,
        auth_token: Optional[str] = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    ) -> None:
        """
        Initialize the RPC client.

        Args:
            host: The hostname or IP address of the RPC server
            port: The port number of the RPC server
            timeout: Socket timeout in seconds
            auth_token: Optional authentication token for rpc_control auth method
            max_response_bytes: Maximum accepted newline-delimited response size
        """
        if not isinstance(host, str) or not host:
            raise ValueError("host must be a non-empty string")
        if type(port) is not int or not 1 <= port <= 65_535:
            raise ValueError("port must be between 1 and 65535")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be greater than zero")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be greater than zero")

        self.host = host
        self.port = port
        self.timeout = float(timeout)
        self.max_response_bytes = max_response_bytes
        self._socket: Optional[socket.socket] = None
        self._receive_buffer = bytearray()
        self._request_id = 0
        self._request_lock = Lock()
        self._connect()
        if auth_token:
            try:
                self.authenticate(auth_token)
            except Exception:
                self.close()
                raise

    def _connect(self) -> None:
        """Establish a connection to the RPC server."""
        try:
            self._socket = socket.create_connection((self.host, self.port), timeout=self.timeout)
            self._socket.settimeout(self.timeout)
        except OSError as exc:
            self.close()
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {exc}") from exc

    def _get_next_id(self) -> int:
        """Get the next request ID."""
        self._request_id += 1
        return self._request_id

    def _receive_line(self) -> bytes:
        """Read one bounded newline-delimited response, retaining any trailing bytes."""
        if self._socket is None:
            raise ConnectionError("Not connected to RPC server")

        while True:
            newline_index = self._receive_buffer.find(b"\n")
            if newline_index >= 0:
                if newline_index > self.max_response_bytes:
                    self.close()
                    raise RPCProtocolError("RPC response exceeds configured size limit")
                response = bytes(self._receive_buffer[:newline_index])
                del self._receive_buffer[: newline_index + 1]
                return response

            if len(self._receive_buffer) > self.max_response_bytes:
                self.close()
                raise RPCProtocolError("RPC response exceeds configured size limit")

            try:
                chunk = self._socket.recv(4096)
            except socket.timeout as exc:
                self.close()
                raise ConnectionError("Request timed out") from exc
            except OSError as exc:
                self.close()
                raise ConnectionError(f"Failed to receive response: {exc}") from exc

            if not chunk:
                self.close()
                raise ConnectionError("Connection closed by server")
            self._receive_buffer.extend(chunk)

    def _send_request(
        self,
        method: str,
        params: Optional[Union[dict[str, Any], list[Any], str]] = None,
    ) -> Any:
        """
        Send a JSON-RPC request and return the result.

        Args:
            method: The RPC method name
            params: The method parameters (can be dict, list, or string)

        Returns:
            The result from the RPC response

        Raises:
            RPCError: If the server returns an error
            ConnectionError: If there's a connection issue
        """
        with self._request_lock:
            if self._socket is None:
                raise ConnectionError("Not connected to RPC server")
            if not method:
                raise ValueError("method must be a non-empty string")

            request_id = self._get_next_id()
            request: dict[str, Any] = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
            }
            if params is not None:
                request["params"] = params

            request_data = json.dumps(request, separators=(",", ":")).encode("utf-8") + b"\n"
            try:
                self._socket.sendall(request_data)
            except OSError as exc:
                self.close()
                raise ConnectionError(f"Failed to send request: {exc}") from exc

            response_data = self._receive_line()
            try:
                response = json.loads(response_data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RPCProtocolError(f"Invalid JSON response: {exc}") from exc

            if not isinstance(response, dict):
                raise RPCProtocolError("JSON-RPC response must be an object")
            if response.get("jsonrpc") != "2.0":
                raise RPCProtocolError("JSON-RPC response has an invalid version")
            response_id = response.get("id")
            if type(response_id) is not int or response_id != request_id:
                raise RPCProtocolError(
                    f"JSON-RPC response ID mismatch: expected {request_id}, got {response_id!r}"
                )

            has_error = "error" in response
            has_result = "result" in response
            if has_error == has_result:
                raise RPCProtocolError(
                    "JSON-RPC response must contain exactly one of result or error"
                )
            if has_error:
                error = response["error"]
                if not isinstance(error, dict):
                    raise RPCProtocolError("JSON-RPC error must be an object")
                code = error.get("code")
                message = error.get("message")
                if type(code) is not int or not isinstance(message, str):
                    raise RPCProtocolError(
                        "JSON-RPC error must contain integer code and string message"
                    )
                raise RPCError(code=code, message=message, data=error.get("data"))

            return response["result"]

    @staticmethod
    def _address_params(address: AddressInput) -> JsonObject:
        """Validate and normalize a single address or address list."""
        if isinstance(address, str):
            if not address.strip():
                raise ValueError("address must be a non-empty string")
            return {"address": address}
        if not isinstance(address, list):
            raise ValueError("address must be a string or list of strings")
        if not address:
            raise ValueError("address list must not be empty")
        if any(not isinstance(item, str) or not item.strip() for item in address):
            raise ValueError("all addresses must be non-empty strings")
        return {"addresses": address.copy()}

    @staticmethod
    def _expect_object(result: Any, method: str) -> JsonObject:
        """Require an object result from a method that promises one."""
        if not isinstance(result, dict):
            raise RPCProtocolError(f"{method} result must be an object")
        return result

    @staticmethod
    def _expect_string_list(result: Any, method: str) -> list[str]:
        """Require a list containing only strings."""
        if not isinstance(result, list) or any(not isinstance(item, str) for item in result):
            raise RPCProtocolError(f"{method} result must be a list of strings")
        return result

    @staticmethod
    def _expect_integer(result: Any, method: str) -> int:
        """Require an integer, accepting integral JSON numbers."""
        if isinstance(result, bool) or not isinstance(result, (int, float)):
            raise RPCProtocolError(f"{method} result must be an integer")
        if isinstance(result, float) and not result.is_integer():
            raise RPCProtocolError(f"{method} result must be an integer")
        return int(result)

    def ping(self) -> str:
        """
        Send a ping request to the server.

        Returns:
            The pong response from the server
        """
        result = self._send_request("ping")
        if not isinstance(result, str):
            raise RPCProtocolError("ping result must be a string")
        return result

    def health(self) -> dict[str, Any]:
        """
        Check the health status of the server.

        Returns:
            A dictionary containing health status information
        """
        return self._expect_object(self._send_request("health"), "health")

    def methods(self) -> list[str]:
        """
        Get the list of available RPC methods.

        Returns:
            A list of method names
        """
        result = self._send_request("methods")
        if isinstance(result, dict):
            result = result.get("methods")
        return self._expect_string_list(result, "methods")

    def authenticate(self, token: str) -> dict[str, Any]:
        """
        Authenticate this connection against the rpc_control auth layer.

        Args:
            token: The auth token configured in rpc_control.auth_token

        Returns:
            Auth response dictionary from the server
        """
        if not token:
            raise ValueError("token must be a non-empty string")
        return self._expect_object(self._send_request("auth", {"token": token}), "auth")

    def monitor_add(self, address: AddressInput) -> dict[str, Any]:
        """
        Add one or more addresses to the monitoring set.

        Args:
            address: A single address string or a list of address strings

        Returns:
            Result information from the server
        """
        return self._expect_object(
            self._send_request("monitor_add", self._address_params(address)),
            "monitor_add",
        )

    def add_address(self, address: AddressInput) -> dict[str, Any]:
        """Alias for monitor_add."""
        return self.monitor_add(address)

    def add_addresses(self, addresses: list[str]) -> dict[str, Any]:
        """Alias for monitor_add with a list of addresses."""
        return self.monitor_add(addresses)

    def monitor_remove(self, address: AddressInput) -> dict[str, Any]:
        """
        Remove one or more addresses from the monitoring set.

        Args:
            address: A single address string or a list of address strings

        Returns:
            Result information from the server
        """
        return self._expect_object(
            self._send_request("monitor_remove", self._address_params(address)),
            "monitor_remove",
        )

    def remove_address(self, address: AddressInput) -> dict[str, Any]:
        """Alias for monitor_remove."""
        return self.monitor_remove(address)

    def remove_addresses(self, addresses: list[str]) -> dict[str, Any]:
        """Alias for monitor_remove with a list of addresses."""
        return self.monitor_remove(addresses)

    def monitor_has(self, address: AddressInput) -> Union[bool, dict[str, bool]]:
        """
        Check if one or more addresses are in the monitoring set.

        Args:
            address: A single address string or a list of address strings

        Returns:
            For a single address: boolean indicating if monitored
            For multiple addresses: dict mapping addresses to boolean values
        """
        result = self._expect_object(
            self._send_request("monitor_has", self._address_params(address)),
            "monitor_has",
        )
        present = self._expect_string_list(result.get("present"), "monitor_has.present")
        if isinstance(address, str):
            return address in present
        present_set = set(present)
        return {item: item in present_set for item in address}

    def is_monitored(self, address: AddressInput) -> Union[bool, dict[str, bool]]:
        """Alias for monitor_has."""
        return self.monitor_has(address)

    def monitor_count(self) -> int:
        """
        Get the number of monitored addresses.

        Returns:
            The number of monitored addresses.
        """
        result = self._send_request("monitor_count")
        if isinstance(result, dict):
            result = result.get("count")
        return self._expect_integer(result, "monitor_count")

    def monitor_list(self) -> list[str]:
        """
        Get the list of all monitored addresses.

        Returns:
            A list of address strings
        """
        result = self._send_request("monitor_list")
        if isinstance(result, dict):
            result = result.get("addresses")
        return self._expect_string_list(result, "monitor_list")

    def monitor_clear(self, confirm: bool = False) -> dict[str, Any]:
        """
        Clear all monitored addresses.

        Args:
            confirm: Must be True to actually clear the set (safety check)

        Returns:
            Result information from the server

        Raises:
            ValueError: If confirm is not True
        """
        if not confirm:
            raise ValueError("Must set confirm=True to clear monitored addresses")

        return self._expect_object(
            self._send_request("monitor_clear", {"confirm": True}),
            "monitor_clear",
        )

    def load_addresses_from_file(self, filepath: Union[str, Path]) -> dict[str, Any]:
        """
        Load addresses from a file and add them to the monitoring set.

        Reads addresses from the specified file (one address per line),
        strips whitespace, skips empty lines, removes comments beginning with #,
        and sends them to the server using monitor_add.

        Args:
            filepath: Path to the file containing addresses

        Returns:
            Result information from the server (same as monitor_add)

        Raises:
            FileNotFoundError: If the file does not exist
            IOError: If there's an error reading the file
            ValueError: If no valid addresses found in the file
        """
        try:
            with Path(filepath).open(encoding="utf-8") as address_file:
                addresses = [
                    line
                    for raw_line in address_file
                    if (line := raw_line.partition("#")[0].strip())
                ]
        except FileNotFoundError as exc:
            raise FileNotFoundError(f"Address file not found: {filepath}") from exc
        except OSError as exc:
            raise OSError(f"Error reading address file {filepath}: {exc}") from exc

        if not addresses:
            raise ValueError(f"No valid addresses found in file: {filepath}")

        return self.monitor_add(addresses)

    def close(self) -> None:
        """Close the connection to the RPC server."""
        active_socket = getattr(self, "_socket", None)
        if active_socket is not None:
            self._socket = None
            with suppress(OSError):
                active_socket.close()
        receive_buffer = getattr(self, "_receive_buffer", None)
        if receive_buffer is not None:
            receive_buffer.clear()

    def __enter__(self) -> "RPCClient":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Context manager exit."""
        self.close()

    def __del__(self) -> None:
        """Cleanup when object is destroyed."""
        self.close()


def main() -> None:
    """
    Example usage of the RPC client.

    This demonstrates basic operations with the rpc_control server.
    """
    import os
    import sys

    # Default connection parameters
    host = "127.0.0.1"
    port = 8080
    auth_token = os.getenv("RPC_CONTROL_AUTH_TOKEN")

    # Parse command line arguments if provided
    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])
    if len(sys.argv) > 3:
        auth_token = sys.argv[3]
    if not auth_token:
        print("Missing auth token. Pass it as argv[3] or set RPC_CONTROL_AUTH_TOKEN.")
        return

    try:
        # Create client using context manager for automatic cleanup
        with RPCClient(host=host, port=port, auth_token=auth_token) as client:
            print(f"Connected to RPC server at {host}:{port}")
            print()

            # Test ping
            print("Testing ping...")
            pong = client.ping()
            print(f"  Response: {pong}")
            print()

            # Get available methods
            print("Getting available methods...")
            methods = client.methods()
            print(f"  Available methods: {', '.join(methods)}")
            print()

            # Check health
            print("Checking health...")
            health = client.health()
            print(f"  Health: {json.dumps(health, indent=2)}")
            print()

            # Add a test address
            test_address = "0x1111111111111111111111111111111111111111"
            print(f"Adding address {test_address}...")
            result = client.monitor_add(test_address)
            print(f"  Result: {json.dumps(result, indent=2)}")
            print()

            # Check if address is monitored
            print(f"Checking if {test_address} is monitored...")
            is_monitored = client.monitor_has(test_address)
            print(f"  Is monitored: {is_monitored}")
            print()

            # Get count of monitored addresses
            print("Getting count of monitored addresses...")
            count = client.monitor_count()
            print(f"  Count: {count}")
            print()

            # List all monitored addresses
            print("Listing all monitored addresses...")
            addresses = client.monitor_list()
            print(f"  Addresses: {addresses}")
            print()

            # Add multiple addresses
            test_addresses = [
                "0x2222222222222222222222222222222222222222",
                "0x3333333333333333333333333333333333333333",
            ]
            print(f"Adding multiple addresses: {test_addresses}...")
            result = client.monitor_add(test_addresses)
            print(f"  Result: {json.dumps(result, indent=2)}")
            print()

            # Demonstrate loading addresses from a file
            # Create a temporary file with some test addresses
            import tempfile

            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                temp_file = f.name
                f.write("# This is a comment\n")
                f.write("0x4444444444444444444444444444444444444444\n")
                f.write("\n")  # Empty line
                f.write("0x5555555555555555555555555555555555555555\n")
                f.write("  0x6666666666666666666666666666666666666666  \n")  # With whitespace

            try:
                print(f"Loading addresses from file: {temp_file}...")
                result = client.load_addresses_from_file(temp_file)
                print(f"  Result: {json.dumps(result, indent=2)}")
                print()
            finally:
                # Clean up temp file
                os.unlink(temp_file)

            # List all addresses again
            print("Listing all monitored addresses after adding more...")
            addresses = client.monitor_list()
            print(f"  Addresses: {addresses}")
            print()

            # Remove one address
            print(f"Removing address {test_address}...")
            result = client.monitor_remove(test_address)
            print(f"  Result: {json.dumps(result, indent=2)}")
            print()

            # Final list
            print("Final list of monitored addresses...")
            addresses = client.monitor_list()
            print(f"  Addresses: {addresses}")
            print()

            print("Demo completed successfully!")

    except ConnectionError as e:
        print(f"Connection error: {e}", file=sys.stderr)
        print(f"\nMake sure the rpc_control server is running on {host}:{port}", file=sys.stderr)
        print(
            "You can start it with: zig build run-rpc-control -- --config conf/config.toml",
            file=sys.stderr,
        )
        sys.exit(1)
    except RPCError as e:
        print(f"RPC error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
