import unittest

from s3_browser.tk_view import _parse_size_bytes, _split_size_bytes


class TkViewUtilsTests(unittest.TestCase):
    def test_split_size_bytes_prefers_largest_unit(self):
        self.assertEqual(("1", "GB"), _split_size_bytes(1024 * 1024 * 1024))
        self.assertEqual(("2", "MB"), _split_size_bytes(2 * 1024 * 1024))
        self.assertEqual(("2", "KB"), _split_size_bytes(2 * 1024))
        self.assertEqual(("512", "B"), _split_size_bytes(512))

    def test_split_size_bytes_defaults_for_non_positive(self):
        self.assertEqual(("1", "MB"), _split_size_bytes(0))

    def test_parse_size_bytes_validates_input(self):
        self.assertEqual(2 * 1024 * 1024, _parse_size_bytes("2", "MB"))
        self.assertIsNone(_parse_size_bytes("nope", "MB"))
        self.assertIsNone(_parse_size_bytes("1", "missing"))
        self.assertIsNone(_parse_size_bytes("0", "KB"))


if __name__ == "__main__":
    unittest.main()
