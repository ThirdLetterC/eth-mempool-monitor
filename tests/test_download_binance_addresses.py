import tempfile
import tomllib
import unittest
import zipfile
from pathlib import Path

from python.download_binance_addresses import convert_archive


class ConvertArchiveTests(unittest.TestCase):
    def test_streams_nested_csv_to_toml_and_filters_rows(self) -> None:
        csv_document = """coin,network,address,balance,Height,Third party custodian name
ETH,ETH,0x3e9244ed698263302104546487f46f7b6e04b0dc,1,1,""
BTC,BTC,not-an-ethereum-address,1,1,""
ETH,BSC,0x69535bac9e4ab870bb80f8104846a14b17c316da,1,1,""
ETH,ETH,invalid,1,1,""
ETH,ETH,0xC3A6E7FD1EE69631E4BF106F67570D74A2D46A1A,1,1,""
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "addresses.zip"
            output_path = root / "addresses.toml"
            with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("snapshot/PR01SEP26_Deposit.csv", csv_document)

            stats = convert_archive(archive_path, output_path)
            with output_path.open("rb") as output_file:
                document = tomllib.load(output_file)

        self.assertEqual(
            document["addresses"],
            [
                "0x3e9244ed698263302104546487f46f7b6e04b0dc",
                "0xc3a6e7fd1ee69631e4bf106f67570d74a2d46a1a",
            ],
        )
        self.assertEqual(stats.rows_read, 5)
        self.assertEqual(stats.addresses_written, 2)
        self.assertEqual(stats.rows_filtered, 2)
        self.assertEqual(stats.invalid_addresses, 1)

    def test_rejects_csv_without_required_headers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "addresses.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("PR01SEP26_Deposit.csv", "coin,address\nETH,0x0\n")

            with self.assertRaisesRegex(ValueError, "network"):
                convert_archive(archive_path, root / "addresses.toml")

    def test_rejects_ambiguous_csv_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_path = root / "addresses.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("one/PR01SEP26_Deposit.csv", "coin,network,address\n")
                archive.writestr("two/PR01SEP26_Deposit.csv", "coin,network,address\n")

            with self.assertRaisesRegex(ValueError, "multiple"):
                convert_archive(archive_path, root / "addresses.toml")


if __name__ == "__main__":
    unittest.main()
