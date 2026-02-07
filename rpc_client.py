"""
RPC Client for eth-mempool-monitor rpc_control server.

This module provides a Python client to interact with the JSON-RPC TCP server
exposed by the rpc_control binary. The server manages monitored Ethereum addresses
stored in Redis.

Usage:
    from rpc_client import RPCClient

    # Connect to the RPC server
    client = RPCClient(host='127.0.0.1', port=8080, auth_token='your-token')

    # Add an address to monitor
    result = client.monitor_add('0x1111111111111111111111111111111111111111')

    # Load addresses from a file
    result = client.load_addresses_from_file('addresses.txt')

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
from typing import Any, Dict, List, Optional, Union


class RPCError(Exception):
    """Exception raised when the RPC server returns an error."""
    
    def __init__(self, code: int, message: str, data: Optional[Any] = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"RPC Error {code}: {message}")


class RPCClient:
    """
    JSON-RPC 2.0 client for eth-mempool-monitor rpc_control server.
    
    The server accepts newline-delimited JSON-RPC requests over TCP and
    manages a set of monitored Ethereum addresses in Redis.
    """
    
    def __init__(self, host: str = '127.0.0.1', port: int = 8080,
                 timeout: float = 30.0, auth_token: Optional[str] = None):
        """
        Initialize the RPC client.
        
        Args:
            host: The hostname or IP address of the RPC server
            port: The port number of the RPC server
            timeout: Socket timeout in seconds
            auth_token: Optional authentication token for rpc_control auth method
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.auth_token = auth_token
        self._socket: Optional[socket.socket] = None
        self._request_id = 0
        self._connect()
        if self.auth_token:
            try:
                self.authenticate(self.auth_token)
            except Exception:
                self.close()
                raise
    
    def _connect(self) -> None:
        """Establish a connection to the RPC server."""
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(self.timeout)
            self._socket.connect((self.host, self.port))
        except socket.error as e:
            raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")
    
    def _get_next_id(self) -> int:
        """Get the next request ID."""
        self._request_id += 1
        return self._request_id
    
    def _send_request(self, method: str, params: Optional[Union[Dict, List, str]] = None) -> Any:
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
        if self._socket is None:
            raise ConnectionError("Not connected to RPC server")
        
        # Build the JSON-RPC 2.0 request
        request: Dict[str, Any] = {
            'jsonrpc': '2.0',
            'id': self._get_next_id(),
            'method': method
        }
        
        if params is not None:
            request['params'] = params
        
        # Serialize and send the request (newline-delimited)
        request_str = json.dumps(request) + '\n'
        
        try:
            self._socket.sendall(request_str.encode('utf-8'))
        except socket.error as e:
            raise ConnectionError(f"Failed to send request: {e}")
        
        # Receive and parse the response
        try:
            response_data = b''
            while True:
                chunk = self._socket.recv(4096)
                if not chunk:
                    raise ConnectionError("Connection closed by server")
                response_data += chunk
                if b'\n' in response_data:
                    break
            
            response_str = response_data.decode('utf-8').strip()
            response = json.loads(response_str)
        except socket.timeout:
            raise ConnectionError("Request timed out")
        except socket.error as e:
            raise ConnectionError(f"Failed to receive response: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON response: {e}")
        
        # Check for errors in the response
        if 'error' in response:
            error = response['error']
            raise RPCError(
                code=error.get('code', -1),
                message=error.get('message', 'Unknown error'),
                data=error.get('data')
            )
        
        # Return the result
        return response.get('result')
    
    def ping(self) -> str:
        """
        Send a ping request to the server.
        
        Returns:
            The pong response from the server
        """
        return self._send_request('ping')
    
    def health(self) -> Dict[str, Any]:
        """
        Check the health status of the server.
        
        Returns:
            A dictionary containing health status information
        """
        return self._send_request('health')
    
    def methods(self) -> List[str]:
        """
        Get the list of available RPC methods.
        
        Returns:
            A list of method names
        """
        return self._send_request('methods')

    def authenticate(self, token: str) -> Dict[str, Any]:
        """
        Authenticate this connection against the rpc_control auth layer.

        Args:
            token: The auth token configured in rpc_control.auth_token

        Returns:
            Auth response dictionary from the server
        """
        if not token:
            raise ValueError("token must be a non-empty string")
        return self._send_request('auth', {'token': token})
    
    def monitor_add(self, address: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Add one or more addresses to the monitoring set.
        
        Args:
            address: A single address string or a list of address strings
        
        Returns:
            Result information from the server
        """
        if isinstance(address, str):
            params = {'address': address}
        elif isinstance(address, list):
            params = {'addresses': address}
        else:
            raise ValueError("address must be a string or list of strings")
        
        return self._send_request('monitor_add', params)
    
    def add_address(self, address: Union[str, List[str]]) -> Dict[str, Any]:
        """Alias for monitor_add."""
        return self.monitor_add(address)
    
    def add_addresses(self, addresses: List[str]) -> Dict[str, Any]:
        """Alias for monitor_add with a list of addresses."""
        return self.monitor_add(addresses)
    
    def monitor_remove(self, address: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Remove one or more addresses from the monitoring set.
        
        Args:
            address: A single address string or a list of address strings
        
        Returns:
            Result information from the server
        """
        if isinstance(address, str):
            params = {'address': address}
        elif isinstance(address, list):
            params = {'addresses': address}
        else:
            raise ValueError("address must be a string or list of strings")
        
        return self._send_request('monitor_remove', params)
    
    def remove_address(self, address: Union[str, List[str]]) -> Dict[str, Any]:
        """Alias for monitor_remove."""
        return self.monitor_remove(address)
    
    def remove_addresses(self, addresses: List[str]) -> Dict[str, Any]:
        """Alias for monitor_remove with a list of addresses."""
        return self.monitor_remove(addresses)
    
    def monitor_has(self, address: Union[str, List[str]]) -> Union[bool, Dict[str, bool]]:
        """
        Check if one or more addresses are in the monitoring set.
        
        Args:
            address: A single address string or a list of address strings
        
        Returns:
            For a single address: boolean indicating if monitored
            For multiple addresses: dict mapping addresses to boolean values
        """
        if isinstance(address, str):
            params = {'address': address}
        elif isinstance(address, list):
            params = {'addresses': address}
        else:
            raise ValueError("address must be a string or list of strings")
        
        return self._send_request('monitor_has', params)
    
    def is_monitored(self, address: Union[str, List[str]]) -> Union[bool, Dict[str, bool]]:
        """Alias for monitor_has."""
        return self.monitor_has(address)
    
    def monitor_count(self) -> Union[int, Dict[str, Any]]:
        """
        Get the number of monitored addresses.
        
        Returns:
            The monitor_count RPC result.
            Current server behavior returns an object with keys like
            {"set_key": "...", "count": N}.
        """
        return self._send_request('monitor_count')
    
    def monitor_list(self) -> List[str]:
        """
        Get the list of all monitored addresses.
        
        Returns:
            A list of address strings
        """
        return self._send_request('monitor_list')
    
    def monitor_clear(self, confirm: bool = False) -> Dict[str, Any]:
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
        
        return self._send_request('monitor_clear', {'confirm': True})
    
    def load_addresses_from_file(self, filepath: str) -> Dict[str, Any]:
        """
        Load addresses from a file and add them to the monitoring set.
        
        Reads addresses from the specified file (one address per line),
        strips whitespace, skips empty lines and comments (lines starting with #),
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
            with open(filepath, 'r') as f:
                addresses = []
                for line in f:
                    # Strip whitespace
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        addresses.append(line)
        except FileNotFoundError:
            raise FileNotFoundError(f"Address file not found: {filepath}")
        except IOError as e:
            raise IOError(f"Error reading address file {filepath}: {e}")
        
        if not addresses:
            raise ValueError(f"No valid addresses found in file: {filepath}")
        
        # Use the existing monitor_add method to send addresses to server
        return self.monitor_add(addresses)
    
    def close(self) -> None:
        """Close the connection to the RPC server."""
        # hasattr check needed for objects created with __new__ that skip __init__
        if hasattr(self, '_socket') and self._socket:
            try:
                self._socket.close()
            except socket.error:
                pass
            finally:
                self._socket = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __del__(self):
        """Cleanup when object is destroyed."""
        self.close()


def main():
    """
    Example usage of the RPC client.
    
    This demonstrates basic operations with the rpc_control server.
    """
    import os
    import sys
    
    # Default connection parameters
    host = '127.0.0.1'
    port = 8080
    auth_token = os.getenv('RPC_CONTROL_AUTH_TOKEN')
    
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
            test_address = '0x1111111111111111111111111111111111111111'
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
                '0x2222222222222222222222222222222222222222',
                '0x3333333333333333333333333333333333333333'
            ]
            print(f"Adding multiple addresses: {test_addresses}...")
            result = client.monitor_add(test_addresses)
            print(f"  Result: {json.dumps(result, indent=2)}")
            print()
            
            # Demonstrate loading addresses from a file
            # Create a temporary file with some test addresses
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
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
        print(f"You can start it with: zig build run-rpc-control -- --config config.toml", file=sys.stderr)
        sys.exit(1)
    except RPCError as e:
        print(f"RPC error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
