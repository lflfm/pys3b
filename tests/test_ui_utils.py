import unittest

from s3_browser.ui_utils import (
    parse_duration_seconds,
    parse_size_bytes,
    split_duration_seconds,
    split_size_bytes,
)


class UiUtilsTests(unittest.TestCase):
    def test_split_size_bytes_prefers_largest_unit(self):
        self.assertEqual(("1", "GB"), split_size_bytes(1024 * 1024 * 1024))
        self.assertEqual(("2", "MB"), split_size_bytes(2 * 1024 * 1024))
        self.assertEqual(("2", "KB"), split_size_bytes(2 * 1024))
        self.assertEqual(("512", "B"), split_size_bytes(512))

    def test_split_size_bytes_defaults_for_non_positive(self):
        self.assertEqual(("1", "MB"), split_size_bytes(0))

    def test_parse_size_bytes_validates_input(self):
        self.assertEqual(2 * 1024 * 1024, parse_size_bytes("2", "MB"))
        self.assertIsNone(parse_size_bytes("nope", "MB"))
        self.assertIsNone(parse_size_bytes("1", "missing"))
        self.assertIsNone(parse_size_bytes("0", "KB"))

    def test_split_duration_seconds_prefers_largest_unit(self):
        self.assertEqual(("2", "Days"), split_duration_seconds(2 * 24 * 60 * 60))
        self.assertEqual(("3", "Hours"), split_duration_seconds(3 * 60 * 60))
        self.assertEqual(("15", "Minutes"), split_duration_seconds(15 * 60))
        self.assertEqual(("45", "Seconds"), split_duration_seconds(45))

    def test_split_duration_seconds_defaults_for_non_positive(self):
        self.assertEqual(("1", "Hours"), split_duration_seconds(0))

    def test_parse_duration_seconds_validates_input(self):
        self.assertEqual(7200, parse_duration_seconds("2", "Hours"))
        self.assertIsNone(parse_duration_seconds("nope", "Hours"))
        self.assertIsNone(parse_duration_seconds("1", "Missing"))
        self.assertIsNone(parse_duration_seconds("0", "Seconds"))


if __name__ == "__main__":
    unittest.main()
