import tempfile
import unittest
from pathlib import Path

from s3_browser.controller import NotConnectedError, S3BrowserController
from s3_browser.models import BucketListing, ObjectDetails
from s3_browser.profiles import ConnectionProfile, ProfileStorage


class FakeService:
    def __init__(self):
        self.buckets = ["bucket-one"]
        self.list_buckets_calls = []
        self.list_objects_calls = []
        self.bucket_listing = BucketListing(name="bucket-one", pages=[])
        self.object_details_calls = []
        self.object_details = ObjectDetails(bucket="bucket-one", key="file.txt")
        self.download_calls = []
        self.upload_calls = []

    def list_buckets(self, *, endpoint_url: str, access_key: str, secret_key: str):
        self.list_buckets_calls.append(
            {
                "endpoint_url": endpoint_url,
                "access_key": access_key,
                "secret_key": secret_key,
            }
        )
        return self.buckets

    def list_objects_for_bucket(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        max_keys: int = 10,
        prefix: str = "",
        delimiter: str | None = "/",
        continuation_token: str | None = None,
    ):
        self.list_objects_calls.append(
            {
                "endpoint_url": endpoint_url,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "max_keys": max_keys,
                "prefix": prefix,
                "delimiter": delimiter,
                "continuation_token": continuation_token,
            }
        )
        return self.bucket_listing

    def get_object_details(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
    ):
        self.object_details_calls.append(
            {
                "endpoint_url": endpoint_url,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "key": key,
            }
        )
        return self.object_details

    def download_object(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
        destination: str,
        progress_callback=None,
        cancel_requested=None,
    ):
        self.download_calls.append(
            {
                "endpoint_url": endpoint_url,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "key": key,
                "destination": destination,
            }
        )
        if progress_callback:
            progress_callback(0)

    def upload_object(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
        source_path: str,
        multipart_threshold: int | None = None,
        multipart_chunk_size: int | None = None,
        max_concurrency: int | None = None,
        progress_callback=None,
        cancel_requested=None,
    ):
        self.upload_calls.append(
            {
                "endpoint_url": endpoint_url,
                "access_key": access_key,
                "secret_key": secret_key,
                "bucket_name": bucket_name,
                "key": key,
                "source_path": source_path,
                "multipart_threshold": multipart_threshold,
                "multipart_chunk_size": multipart_chunk_size,
                "max_concurrency": max_concurrency,
            }
        )
        if progress_callback:
            progress_callback(0)


class FakeProfileStorage:
    def __init__(self, profiles=None):
        self._profiles = list(profiles or [])
        self.saved_snapshots: list[list[ConnectionProfile]] = []

    def load(self):
        return list(self._profiles)

    def save(self, profiles):
        snapshot = [ConnectionProfile(**profile.__dict__) for profile in profiles]
        self.saved_snapshots.append(snapshot)
        self._profiles = snapshot


class S3BrowserControllerTests(unittest.TestCase):
    def setUp(self):
        self.fake_service = FakeService()
        self.storage = FakeProfileStorage()
        self.controller = S3BrowserController(service=self.fake_service, storage=self.storage)
        self.params = {
            "endpoint_url": "https://example.com",
            "access_key": "access",
            "secret_key": "secret",
        }

    def test_connect_stores_connection_and_returns_buckets(self):
        buckets = self.controller.connect(**self.params)

        self.assertEqual(["bucket-one"], buckets)
        self.assertTrue(self.controller.is_connected)
        self.assertEqual(1, len(self.fake_service.list_buckets_calls))
        self.assertEqual(self.params, self.fake_service.list_buckets_calls[0])

    def test_refresh_requires_existing_connection(self):
        with self.assertRaises(NotConnectedError):
            self.controller.refresh_buckets()

        self.controller.connect(**self.params)
        self.fake_service.buckets = ["other"]

        buckets = self.controller.refresh_buckets()

        self.assertEqual(["other"], buckets)
        self.assertEqual(2, len(self.fake_service.list_buckets_calls))
        self.assertEqual(self.params, self.fake_service.list_buckets_calls[1])

    def test_list_objects_requires_connection_and_passes_params(self):
        with self.assertRaises(NotConnectedError):
            self.controller.list_objects(bucket_name="bucket-one")

        self.controller.connect(**self.params)
        listing = self.controller.list_objects(bucket_name="bucket-one", max_keys=25)

        self.assertIs(listing, self.fake_service.bucket_listing)
        self.assertEqual(1, len(self.fake_service.list_objects_calls))
        self.assertEqual(
            {
                **self.params,
                "bucket_name": "bucket-one",
                "max_keys": 25,
                "prefix": "",
                "delimiter": "/",
                "continuation_token": None,
            },
            self.fake_service.list_objects_calls[0],
        )

    def test_list_objects_supports_prefix(self):
        self.controller.connect(**self.params)

        self.controller.list_objects(bucket_name="bucket-one", prefix="folder/")

        self.assertEqual("folder/", self.fake_service.list_objects_calls[0]["prefix"])

    def test_list_objects_supports_continuation_token(self):
        self.controller.connect(**self.params)

        self.controller.list_objects(bucket_name="bucket-one", continuation_token="token-1")

        self.assertEqual("token-1", self.fake_service.list_objects_calls[0]["continuation_token"])

    def test_get_object_details_requires_connection(self):
        with self.assertRaises(NotConnectedError):
            self.controller.get_object_details(bucket_name="bucket-one", key="file.txt")

    def test_get_object_details_passes_through_params(self):
        self.controller.connect(**self.params)

        details = self.controller.get_object_details(bucket_name="bucket-one", key="file.txt")

        self.assertIs(details, self.fake_service.object_details)
        self.assertEqual(1, len(self.fake_service.object_details_calls))
        self.assertEqual(
            {
                **self.params,
                "bucket_name": "bucket-one",
                "key": "file.txt",
            },
            self.fake_service.object_details_calls[0],
        )

    def test_download_object_requires_connection(self):
        with self.assertRaises(NotConnectedError):
            self.controller.download_object(bucket_name="bucket-one", key="file.txt", destination="/tmp/file.txt")

    def test_download_object_passes_through_params(self):
        self.controller.connect(**self.params)

        self.controller.download_object(
            bucket_name="bucket-one",
            key="file.txt",
            destination="/tmp/file.txt",
        )

        self.assertEqual(1, len(self.fake_service.download_calls))
        self.assertEqual(
            {
                **self.params,
                "bucket_name": "bucket-one",
                "key": "file.txt",
                "destination": "/tmp/file.txt",
            },
            self.fake_service.download_calls[0],
        )

    def test_upload_object_requires_connection(self):
        with self.assertRaises(NotConnectedError):
            self.controller.upload_object(
                bucket_name="bucket-one",
                key="file.txt",
                source_path="/tmp/file.txt",
            )

    def test_upload_object_passes_through_params(self):
        self.controller.connect(**self.params)

        self.controller.upload_object(
            bucket_name="bucket-one",
            key="file.txt",
            source_path="/tmp/file.txt",
            multipart_threshold=123,
            multipart_chunk_size=456,
            max_concurrency=7,
        )

        self.assertEqual(1, len(self.fake_service.upload_calls))
        self.assertEqual(
            {
                **self.params,
                "bucket_name": "bucket-one",
                "key": "file.txt",
                "source_path": "/tmp/file.txt",
                "multipart_threshold": 123,
                "multipart_chunk_size": 456,
                "max_concurrency": 7,
            },
            self.fake_service.upload_calls[0],
        )

    def test_loads_profiles_from_storage_on_init(self):
        profiles = [
            ConnectionProfile(
                name="alpha",
                endpoint_url="https://one",
                access_key="a",
                secret_key="b",
            )
        ]
        storage = FakeProfileStorage(profiles)
        controller = S3BrowserController(service=self.fake_service, storage=storage)

        self.assertEqual(profiles, controller.list_profiles())

    def test_save_profile_creates_and_updates_profiles(self):
        profile = ConnectionProfile(
            name="alpha",
            endpoint_url="https://one",
            access_key="a",
            secret_key="b",
        )
        self.controller.save_profile(profile)
        self.assertEqual([profile], self.controller.list_profiles())

        updated = ConnectionProfile(
            name="alpha",
            endpoint_url="https://two",
            access_key="c",
            secret_key="d",
        )
        self.controller.save_profile(updated)
        self.assertEqual([updated], self.controller.list_profiles())
        self.assertEqual(updated.endpoint_url, self.storage._profiles[0].endpoint_url)

    def test_save_profile_supports_renaming(self):
        existing = ConnectionProfile(
            name="alpha",
            endpoint_url="https://one",
            access_key="a",
            secret_key="b",
        )
        self.controller.save_profile(existing)
        renamed = ConnectionProfile(
            name="beta",
            endpoint_url="https://one",
            access_key="a",
            secret_key="b",
        )
        self.controller.save_profile(renamed, original_name="alpha")
        self.assertEqual([renamed], self.controller.list_profiles())

    def test_delete_profile_removes_and_persists(self):
        profile = ConnectionProfile(
            name="alpha",
            endpoint_url="https://one",
            access_key="a",
            secret_key="b",
        )
        self.controller.save_profile(profile)
        self.controller.delete_profile("alpha")
        self.assertEqual([], self.controller.list_profiles())
        self.assertEqual([], self.storage._profiles)
        with self.assertRaises(ValueError):
            self.controller.delete_profile("missing")

    def test_connect_with_profile_uses_saved_credentials(self):
        profile = ConnectionProfile(
            name="alpha",
            endpoint_url="https://example",
            access_key="ak",
            secret_key="sk",
        )
        self.controller.save_profile(profile)

        buckets = self.controller.connect_with_profile("alpha")

        self.assertEqual(["bucket-one"], buckets)
        self.assertEqual(1, len(self.fake_service.list_buckets_calls))
        self.assertEqual(
            {
                "endpoint_url": "https://example",
                "access_key": "ak",
                "secret_key": "sk",
            },
            self.fake_service.list_buckets_calls[0],
        )


class ProfileStorageTests(unittest.TestCase):
    def test_save_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connections.json"
            storage = ProfileStorage(path)
            profiles = [
                ConnectionProfile(name="alpha", endpoint_url="https://one", access_key="a", secret_key="b"),
                ConnectionProfile(name="beta", endpoint_url="https://two", access_key="c", secret_key="d"),
            ]
            storage.save(profiles)

            loaded = storage.load()

            self.assertEqual(profiles, loaded)

    def test_load_returns_empty_on_invalid_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connections.json"
            path.write_text("not json", encoding="utf-8")
            storage = ProfileStorage(path)

            self.assertEqual([], storage.load())


if __name__ == "__main__":
    unittest.main()
