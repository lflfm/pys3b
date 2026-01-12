import json
import tempfile
import unittest
from pathlib import Path

from s3_browser.settings import AppSettings, SettingsStorage


class SettingsStorageTests(unittest.TestCase):
    def test_load_returns_defaults_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "settings.json"
            storage = SettingsStorage(path)

            settings = storage.load()

            self.assertEqual(AppSettings(), settings)

    def test_load_sanitizes_invalid_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "settings.json"
            payload = {
                "fetch_limit": "nope",
                "default_post_max_size": -1,
                "upload_multipart_threshold": 0,
                "upload_chunk_size": "bad",
                "upload_max_concurrency": -5,
                "remember_last_bucket": "yes",
                "last_bucket": 123,
                "last_connection": None,
            }
            path.write_text(json.dumps(payload), encoding="utf-8")
            storage = SettingsStorage(path)

            settings = storage.load()

            self.assertEqual(AppSettings.fetch_limit, settings.fetch_limit)
            self.assertEqual(AppSettings.default_post_max_size, settings.default_post_max_size)
            self.assertEqual(AppSettings.upload_multipart_threshold, settings.upload_multipart_threshold)
            self.assertEqual(AppSettings.upload_chunk_size, settings.upload_chunk_size)
            self.assertEqual(AppSettings.upload_max_concurrency, settings.upload_max_concurrency)
            self.assertEqual("", settings.last_bucket)
            self.assertEqual("", settings.last_connection)

    def test_save_sanitizes_minimum_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "settings.json"
            storage = SettingsStorage(path)
            settings = AppSettings(
                fetch_limit=0,
                default_post_max_size=-1,
                upload_multipart_threshold=0,
                upload_chunk_size=-5,
                upload_max_concurrency=0,
                remember_last_bucket=True,
                last_bucket="bucket",
                last_connection="conn",
            )

            storage.save(settings)

            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(1, saved["fetch_limit"])
            self.assertEqual(1, saved["default_post_max_size"])
            self.assertEqual(1, saved["upload_multipart_threshold"])
            self.assertEqual(1, saved["upload_chunk_size"])
            self.assertEqual(1, saved["upload_max_concurrency"])
            self.assertTrue(saved["remember_last_bucket"])
            self.assertEqual("bucket", saved["last_bucket"])
            self.assertEqual("conn", saved["last_connection"])


if __name__ == "__main__":
    unittest.main()
