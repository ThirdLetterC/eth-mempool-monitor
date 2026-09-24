"""Unit tests for the newline-delimited JSON-RPC client."""

import json
import tempfile
import unittest
from collections import deque
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from python.rpc_client import RPCClient, RPCError, RPCProtocolError


class StubSocket:
    """Deterministic socket stub that records writes and scripts reads."""

    def __init__(self, *responses: bytes) -> None:
        self.responses = deque(responses)
        self.sent: list[bytes] = []
        self.timeout: float | None = None
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def recv(self, size: int) -> bytes:
        del size
        return self.responses.popleft() if self.responses else b""

    def close(self) -> None:
        self.closed = True


def response(request_id: int, *, result: object) -> bytes:
    """Create a newline-delimited JSON-RPC success response."""
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}).encode() + b"\n"


class RPCClientTests(unittest.TestCase):
    """Exercise framing, validation, and resource-management behavior."""

    def create_client(
        self,
        stub: StubSocket,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        timeout: float = 30.0,
        auth_token: str | None = None,
        max_response_bytes: int = 4 * 1024 * 1024,
    ) -> RPCClient:
        with patch("python.rpc_client.socket.create_connection", return_value=stub):
            return RPCClient(
                host=host,
                port=port,
                timeout=timeout,
                auth_token=auth_token,
                max_response_bytes=max_response_bytes,
            )

    def test_partial_and_buffered_responses(self) -> None:
        first = response(1, result="pong")
        second = response(2, result=["ping", "methods"])
        split = len(first) // 2
        stub = StubSocket(first[:split], first[split:] + second)
        client = self.create_client(stub)

        self.assertEqual(client.ping(), "pong")
        self.assertEqual(client.methods(), ["ping", "methods"])
        self.assertEqual(len(stub.sent), 2)
        self.assertEqual(json.loads(stub.sent[0]), {"jsonrpc": "2.0", "id": 1, "method": "ping"})

    def test_rpc_error_preserves_server_details(self) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32_000, "message": "denied", "data": {"retry": False}},
        }
        stub = StubSocket(json.dumps(payload).encode() + b"\n")
        client = self.create_client(stub)

        with self.assertRaisesRegex(RPCError, "denied") as raised:
            client.ping()

        self.assertEqual(raised.exception.code, -32_000)
        self.assertEqual(raised.exception.data, {"retry": False})

    def test_server_result_wrappers_are_normalized(self) -> None:
        addresses = ["0x1", "0x2"]
        stub = StubSocket(
            response(1, result={"methods": ["ping", "methods"], "count": 2}),
            response(2, result={"set_key": "monitored", "count": 2}),
            response(3, result={"set_key": "monitored", "count": 2, "addresses": addresses}),
            response(4, result={"present": ["0x1"], "absent": ["0x2"], "invalid": []}),
            response(5, result={"present": ["0x1"], "absent": [], "invalid": []}),
        )
        client = self.create_client(stub)

        self.assertEqual(client.methods(), ["ping", "methods"])
        self.assertEqual(client.monitor_count(), 2)
        self.assertEqual(client.monitor_list(), addresses)
        self.assertEqual(client.monitor_has(addresses), {"0x1": True, "0x2": False})
        self.assertTrue(client.monitor_has("0x1"))

    def test_mismatched_response_id_is_rejected(self) -> None:
        client = self.create_client(StubSocket(response(99, result="pong")))

        with self.assertRaisesRegex(RPCProtocolError, "ID mismatch"):
            client.ping()

    def test_oversized_response_closes_connection(self) -> None:
        stub = StubSocket(b"x" * 33 + b"\n")
        client = self.create_client(stub, max_response_bytes=32)

        with self.assertRaisesRegex(RPCProtocolError, "size limit"):
            client.ping()

        self.assertTrue(stub.closed)

    def test_timeout_closes_connection(self) -> None:
        class TimeoutSocket(StubSocket):
            def recv(self, size: int) -> bytes:
                del size
                raise TimeoutError

        stub = TimeoutSocket()
        client = self.create_client(stub)

        with self.assertRaisesRegex(ConnectionError, "timed out"):
            client.ping()

        self.assertTrue(stub.closed)

    def test_address_validation_happens_before_network_write(self) -> None:
        stub = StubSocket()
        client = self.create_client(stub)

        invalid_values: list[object] = ["", "  ", [], ["valid", ""], [1]]
        for value in invalid_values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                client.monitor_add(cast(Any, value))

        self.assertEqual(stub.sent, [])

    def test_load_addresses_from_toml(self) -> None:
        stub = StubSocket(response(1, result={"added": ["0x1", "0x2"]}))
        client = self.create_client(stub)

        with tempfile.TemporaryDirectory() as directory:
            address_file = Path(directory, "addresses.toml")
            address_file.write_text('addresses = ["0x1", "0x2"]\n', encoding="utf-8")
            result = client.load_addresses_from_file(address_file)

        request = json.loads(stub.sent[0])
        self.assertEqual(request["params"], {"addresses": ["0x1", "0x2"]})
        self.assertEqual(result, {"added": ["0x1", "0x2"]})

    def test_load_addresses_rejects_invalid_toml_schema(self) -> None:
        stub = StubSocket()
        client = self.create_client(stub)

        with tempfile.TemporaryDirectory() as directory:
            address_file = Path(directory, "addresses.toml")
            invalid_documents = [
                "other = []\n",
                "addresses = []\n",
                'addresses = ["0x1", 2]\n',
                'addresses = ["0x1", ""]\n',
            ]
            for document in invalid_documents:
                with self.subTest(document=document):
                    address_file.write_text(document, encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "addresses"):
                        client.load_addresses_from_file(address_file)

        self.assertEqual(stub.sent, [])

    def test_load_addresses_rejects_large_file_with_batched_hint(self) -> None:
        client = self.create_client(StubSocket())

        with tempfile.TemporaryDirectory() as directory:
            address_file = Path(directory, "addresses.toml")
            address_file.write_text('addresses = ["0x1"]\n', encoding="utf-8")
            with (
                patch("python.rpc_client.MAX_IN_MEMORY_ADDRESS_FILE_BYTES", 1),
                self.assertRaisesRegex(ValueError, "load_addresses_from_file_batched"),
            ):
                client.load_addresses_from_file(address_file)

    def test_load_addresses_batched_streams_large_canonical_toml(self) -> None:
        addresses = [f"0x{index:040x}" for index in range(5)]
        stub = StubSocket(
            response(
                1,
                result={
                    "added_count": 2,
                    "already_present_count": 0,
                    "invalid_count": 0,
                },
            ),
            response(
                2,
                result={
                    "added_count": 1,
                    "already_present_count": 1,
                    "invalid_count": 0,
                },
            ),
            response(
                3,
                result={
                    "added_count": 0,
                    "already_present_count": 1,
                    "invalid_count": 0,
                },
            ),
        )
        client = self.create_client(stub)
        progress: list[tuple[int, int]] = []

        with tempfile.TemporaryDirectory() as directory:
            address_file = Path(directory, "addresses.toml")
            address_file.write_text(
                "# Generated address file\naddresses = [\n"
                + "".join(f'    "{address}",\n' for address in addresses)
                + "]\n",
                encoding="utf-8",
            )
            with patch("python.rpc_client.MAX_IN_MEMORY_ADDRESS_FILE_BYTES", 1):
                result = client.load_addresses_from_file_batched(
                    address_file,
                    batch_size=2,
                    progress=lambda batches, processed: progress.append((batches, processed)),
                )

        requests = [json.loads(request) for request in stub.sent]
        self.assertEqual(
            [request["params"]["addresses"] for request in requests],
            [addresses[:2], addresses[2:4], addresses[4:]],
        )
        self.assertEqual(
            result,
            {
                "requested_count": 5,
                "added_count": 3,
                "already_present_count": 2,
                "invalid_count": 0,
                "batch_count": 3,
            },
        )
        self.assertEqual(progress, [(1, 2), (2, 4), (3, 5)])

    def test_load_addresses_batched_rejects_noncanonical_large_toml(self) -> None:
        stub = StubSocket()
        client = self.create_client(stub)

        with tempfile.TemporaryDirectory() as directory:
            address_file = Path(directory, "addresses.toml")
            invalid_documents = [
                'addresses = ["0x1"]\n',
                'addresses = [\n"0x0000000000000000000000000000000000000001",\n',
            ]
            for document in invalid_documents:
                with self.subTest(document=document):
                    address_file.write_text(document, encoding="utf-8")
                    with (
                        patch("python.rpc_client.MAX_IN_MEMORY_ADDRESS_FILE_BYTES", 1),
                        self.assertRaises(ValueError),
                    ):
                        client.load_addresses_from_file_batched(address_file)

        self.assertEqual(stub.sent, [])

    def test_load_addresses_batched_rejects_unsafe_batch_size(self) -> None:
        client = self.create_client(StubSocket())

        for batch_size in (0, 1_001):
            with (
                self.subTest(batch_size=batch_size),
                self.assertRaisesRegex(ValueError, "batch_size"),
            ):
                client.load_addresses_from_file_batched("unused.toml", batch_size=batch_size)

    def test_constructor_rejects_invalid_limits(self) -> None:
        with self.assertRaises(ValueError):
            RPCClient(host="")
        with self.assertRaises(ValueError):
            RPCClient(port=0)
        with self.assertRaises(ValueError):
            RPCClient(port=65_536)
        with self.assertRaises(ValueError):
            RPCClient(timeout=0)
        with self.assertRaises(ValueError):
            RPCClient(max_response_bytes=0)
        with self.assertRaises(ValueError):
            RPCClient(timeout=float("inf"))
        with self.assertRaises(ValueError):
            RPCClient(port=cast(Any, "8080"))


if __name__ == "__main__":
    unittest.main()
