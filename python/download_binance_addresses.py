"""Download Binance's proof-of-reserves archive and export Ethereum addresses.

The archive is copied to temporary disk in bounded-size chunks.  The selected CSV
member is then decompressed and parsed row-by-row, so neither large file is held
in memory.
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, Any

DEFAULT_API_URL = (
    "https://www.binance.com/bapi/apex/v1/public/apex/market/por/getDownloadUrl?auditId=PR01SEP26"
)
DEFAULT_CSV_NAME = "PR01SEP26_Deposit.csv"
DEFAULT_OUTPUT = Path("conf/binance_addresses.toml")
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
MAX_API_RESPONSE_SIZE = 1024 * 1024
MAX_ARCHIVE_SIZE = 5 * 1024 * 1024 * 1024
MAX_CSV_SIZE = 5 * 1024 * 1024 * 1024
ETH_ADDRESS_PATTERN = re.compile(r"0x[0-9a-fA-F]{40}\Z")
USER_AGENT = "eth-mempool-monitor-binance-address-importer/1.0"


@dataclass(frozen=True, slots=True)
class ConversionStats:
    """Counts collected while streaming the source CSV."""

    rows_read: int
    addresses_written: int
    rows_filtered: int
    invalid_addresses: int


def _open_url(url: str, timeout: float) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json, application/zip", "User-Agent": USER_AGENT},
    )
    return urllib.request.urlopen(request, timeout=timeout)


def _require_https_url(value: object, description: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{description} is missing or is not a string")

    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{description} must be an HTTPS URL")
    return value


def fetch_download_url(api_url: str, timeout: float) -> str:
    """Resolve Binance's API endpoint to its temporary archive URL."""
    _require_https_url(api_url, "API URL")
    with _open_url(api_url, timeout) as response:
        body = response.read(MAX_API_RESPONSE_SIZE + 1)

    if len(body) > MAX_API_RESPONSE_SIZE:
        raise ValueError("Binance API response exceeds the 1 MiB safety limit")

    payload: Any = json.loads(body)
    if not isinstance(payload, Mapping):
        raise ValueError("Binance API returned a non-object JSON response")
    if payload.get("success") is not True or payload.get("code") != "000000":
        raise ValueError(
            f"Binance API rejected the request: code={payload.get('code')!r}, "
            f"message={payload.get('message')!r}"
        )
    return _require_https_url(payload.get("data"), "Binance archive URL")


def download_archive(url: str, destination: Path, timeout: float) -> int:
    """Stream an HTTPS archive into *destination* and return its byte count."""
    _require_https_url(url, "Archive URL")
    with _open_url(url, timeout) as response, destination.open("wb") as archive_file:
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                declared_size = int(content_length)
            except ValueError as exc:
                raise ValueError("Archive has an invalid Content-Length header") from exc
            if declared_size < 0 or declared_size > MAX_ARCHIVE_SIZE:
                raise ValueError("Archive exceeds the 5 GiB safety limit")

        total = 0
        while chunk := response.read(DOWNLOAD_CHUNK_SIZE):
            total += len(chunk)
            if total > MAX_ARCHIVE_SIZE:
                raise ValueError("Archive exceeds the 5 GiB safety limit")
            archive_file.write(chunk)

    return total


def _find_csv_member(archive: zipfile.ZipFile, csv_name: str) -> zipfile.ZipInfo:
    matches = [
        member
        for member in archive.infolist()
        if not member.is_dir() and PurePosixPath(member.filename).name == csv_name
    ]
    if not matches:
        raise ValueError(f"Archive does not contain {csv_name!r}")
    if len(matches) != 1:
        raise ValueError(f"Archive contains multiple files named {csv_name!r}")

    member = matches[0]
    if member.flag_bits & 0x1:
        raise ValueError(f"Archive member {member.filename!r} is encrypted")
    if member.file_size > MAX_CSV_SIZE:
        raise ValueError("CSV exceeds the 5 GiB safety limit")
    return member


def _validated_headers(reader: csv.DictReader[str]) -> None:
    required = {"coin", "network", "address"}
    actual = set(reader.fieldnames or ())
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(missing)}")


def _write_toml_from_csv(
    csv_file: IO[bytes],
    output_file: Path,
    coin: str,
    network: str,
) -> ConversionStats:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with (
            io.TextIOWrapper(csv_file, encoding="utf-8-sig", newline="") as text_file,
            tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=output_file.parent,
                prefix=f".{output_file.name}.",
                suffix=".tmp",
                delete=False,
            ) as destination,
        ):
            temporary_path = Path(destination.name)
            reader = csv.DictReader(text_file)
            _validated_headers(reader)
            destination.write("# Generated from Binance proof-of-reserves data.\naddresses = [\n")

            rows_read = 0
            addresses_written = 0
            rows_filtered = 0
            invalid_addresses = 0

            for row in reader:
                rows_read += 1
                row_coin = row.get("coin")
                row_network = row.get("network")
                address_value = row.get("address")
                if (
                    not isinstance(row_coin, str)
                    or not isinstance(row_network, str)
                    or not isinstance(address_value, str)
                ):
                    invalid_addresses += 1
                    continue
                if row_coin.strip().upper() != coin or row_network.strip().upper() != network:
                    rows_filtered += 1
                    continue

                address = address_value.strip()
                if ETH_ADDRESS_PATTERN.fullmatch(address) is None:
                    invalid_addresses += 1
                    continue

                destination.write(f"    {json.dumps(address.lower())},\n")
                addresses_written += 1

            destination.write("]\n")
            destination.flush()
            os.fsync(destination.fileno())

            if addresses_written == 0:
                raise ValueError("CSV contains no valid addresses matching the requested filters")

        os.replace(temporary_path, output_file)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return ConversionStats(rows_read, addresses_written, rows_filtered, invalid_addresses)


def convert_archive(
    archive_path: Path,
    output_file: Path,
    csv_name: str = DEFAULT_CSV_NAME,
    coin: str = "ETH",
    network: str = "ETH",
) -> ConversionStats:
    """Stream one CSV member from a ZIP archive into a TOML address array."""
    with zipfile.ZipFile(archive_path) as archive:
        member = _find_csv_member(archive, csv_name)
        with archive.open(member) as csv_file:
            return _write_toml_from_csv(csv_file, output_file, coin.upper(), network.upper())


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=DEFAULT_API_URL, help="Binance download-URL endpoint")
    parser.add_argument("--csv-name", default=DEFAULT_CSV_NAME, help="CSV basename inside the ZIP")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="destination TOML file")
    parser.add_argument("--coin", default="ETH", help="coin column to include (default: ETH)")
    parser.add_argument("--network", default="ETH", help="network column to include (default: ETH)")
    parser.add_argument(
        "--timeout", type=float, default=60.0, help="network operation timeout in seconds"
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if not args.csv_name or PurePosixPath(args.csv_name).name != args.csv_name:
        parser.error("--csv-name must be a basename, not a path")
    return args


def main() -> int:
    args = _parse_args()
    try:
        print(f"Resolving archive URL from {args.api_url}", file=sys.stderr)
        archive_url = fetch_download_url(args.api_url, args.timeout)

        with tempfile.TemporaryDirectory(prefix="binance-addresses-") as temporary_directory:
            archive_path = Path(temporary_directory, "proof-of-reserves.zip")
            print("Downloading archive to temporary disk...", file=sys.stderr)
            downloaded = download_archive(archive_url, archive_path, args.timeout)
            print(f"Downloaded {downloaded / (1024 * 1024):.1f} MiB", file=sys.stderr)
            stats = convert_archive(
                archive_path,
                args.output,
                args.csv_name,
                args.coin.upper(),
                args.network.upper(),
            )

        print(
            f"Wrote {stats.addresses_written} addresses to {args.output} "
            f"({stats.rows_read} rows read, {stats.rows_filtered} filtered, "
            f"{stats.invalid_addresses} invalid)",
            file=sys.stderr,
        )
        return 0
    except (OSError, ValueError, json.JSONDecodeError, csv.Error, zipfile.BadZipFile) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
