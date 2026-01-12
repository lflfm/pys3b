import unittest
from datetime import datetime

try:
    from botocore.exceptions import ClientError
except ModuleNotFoundError:  # pragma: no cover - fallback for local testing
    from s3_browser.services import ClientError  # type: ignore

from s3_browser import services
from s3_browser.services import S3BrowserService, TransferCancelledError


class FakeS3Client:
    def __init__(
        self,
        buckets,
        object_responses,
        head_object_responses=None,
        download_errors=None,
        upload_errors=None,
        delete_errors=None,
        presigned_url_outputs=None,
        presigned_post_outputs=None,
        transfer_sequences=None,
    ):
        self.buckets = buckets
        self.object_responses = {name: iter(responses) for name, responses in object_responses.items()}
        self.list_objects_calls = []
        self.list_objects_kwargs = []
        self.head_object_calls = []
        self.head_object_responses = head_object_responses or {}
        self.download_file_calls = []
        self.download_file_errors = download_errors or {}
        self.upload_file_calls = []
        self.upload_file_errors = upload_errors or {}
        self.upload_file_configs = []
        self.delete_object_calls = []
        self.delete_object_errors = delete_errors or {}
        self.presigned_url_outputs = presigned_url_outputs or {}
        self.presigned_url_calls = []
        self.presigned_post_outputs = presigned_post_outputs or {}
        self.presigned_post_calls = []
        self.transfer_sequences = transfer_sequences or {}

    def list_buckets(self):
        return {"Buckets": [{"Name": name} for name in self.buckets]}

    def list_objects_v2(self, **kwargs):
        bucket = kwargs["Bucket"]
        continuation = kwargs.get("ContinuationToken")
        self.list_objects_calls.append((bucket, continuation))
        self.list_objects_kwargs.append(kwargs)

        response = next(self.object_responses[bucket])
        if isinstance(response, Exception):
            raise response
        return response

    def head_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        self.head_object_calls.append(kwargs)
        response = self.head_object_responses.get((bucket, key), {})
        if isinstance(response, Exception):
            raise response
        return response

    def download_file(self, bucket, key, filename, Callback=None):
        self.download_file_calls.append((bucket, key, filename))
        error = self.download_file_errors.get((bucket, key))
        if isinstance(error, Exception):
            raise error
        if Callback:
            for amount in self.transfer_sequences.get(("download", bucket, key), []):
                Callback(amount)

    def upload_file(self, filename, bucket, key, Callback=None, ExtraArgs=None, Config=None):
        self.upload_file_calls.append((filename, bucket, key))
        self.upload_file_configs.append(Config)
        error = self.upload_file_errors.get((bucket, key))
        if isinstance(error, Exception):
            raise error
        if Callback:
            for amount in self.transfer_sequences.get(("upload", bucket, key), []):
                Callback(amount)

    def delete_object(self, **kwargs):
        bucket = kwargs["Bucket"]
        key = kwargs["Key"]
        self.delete_object_calls.append((bucket, key))
        error = self.delete_object_errors.get((bucket, key))
        if isinstance(error, Exception):
            raise error

    def generate_presigned_url(self, client_method, Params=None, ExpiresIn=3600):
        params = Params or {}
        self.presigned_url_calls.append(
            {
                "method": client_method,
                "params": params,
                "expires_in": ExpiresIn,
            }
        )
        key = (client_method, params.get("Bucket"), params.get("Key"))
        result = self.presigned_url_outputs.get(key, "signed-url")
        if isinstance(result, Exception):
            raise result
        return result

    def generate_presigned_post(self, Bucket=None, Key=None, Fields=None, Conditions=None, ExpiresIn=3600):
        self.presigned_post_calls.append(
            {
                "bucket": Bucket,
                "key": Key,
                "fields": Fields or {},
                "conditions": Conditions or [],
                "expires_in": ExpiresIn,
            }
        )
        result = self.presigned_post_outputs.get((Bucket, Key), {"url": "post-url", "fields": Fields or {}})
        if isinstance(result, Exception):
            raise result
        return result


class S3BrowserServiceTests(unittest.TestCase):
    def test_lists_paginated_objects_for_multiple_buckets(self):
        object_responses = {
            "bucket-one": [
                {"Contents": [{"Key": "a.txt"}], "IsTruncated": True, "NextContinuationToken": "token-1"},
                {"Contents": [{"Key": "b.txt"}], "IsTruncated": False},
            ],
            "bucket-two": [
                {"Contents": [], "IsTruncated": False},
            ],
        }
        fake_client = FakeS3Client(["bucket-one", "bucket-two"], object_responses)
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        listings = service.list_buckets_with_objects(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            max_keys=10,
        )

        self.assertEqual(2, len(listings))

        bucket_one = listings[0]
        self.assertEqual("bucket-one", bucket_one.name)
        self.assertEqual(["a.txt"], bucket_one.pages[0].keys)
        self.assertEqual(["b.txt"], bucket_one.pages[1].keys)
        self.assertIsNone(bucket_one.error)
        self.assertFalse(bucket_one.has_more)
        self.assertIsNone(bucket_one.continuation_token)

        bucket_two = listings[1]
        self.assertEqual("bucket-two", bucket_two.name)
        self.assertEqual([], bucket_two.pages[0].keys)
        self.assertIsNone(bucket_two.error)

        for kwargs in fake_client.list_objects_kwargs:
            self.assertEqual("/", kwargs.get("Delimiter"))

        self.assertIn(("bucket-one", None), fake_client.list_objects_calls)
        self.assertIn(("bucket-one", "token-1"), fake_client.list_objects_calls)
        bucket_one_calls = [
            kwargs for kwargs in fake_client.list_objects_kwargs if kwargs["Bucket"] == "bucket-one"
        ]
        self.assertEqual([10, 9], [call["MaxKeys"] for call in bucket_one_calls])

    def test_handles_errors_during_listing(self):
        list_error = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Denied"}},
            "ListObjectsV2",
        )
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [list_error]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        listings = service.list_buckets_with_objects(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
        )

        self.assertEqual(1, len(listings))
        bucket = listings[0]
        self.assertEqual("bucket-one", bucket.name)
        self.assertEqual(1, len(bucket.pages))
        self.assertEqual(str(list_error), bucket.pages[0].error)
        self.assertEqual(str(list_error), bucket.error)

    def test_stops_listing_when_limit_reached(self):
        object_responses = {
            "bucket-one": [
                {"Contents": [{"Key": "a.txt"}], "IsTruncated": True, "NextContinuationToken": "token-1"},
                {"Contents": [{"Key": "b.txt"}], "IsTruncated": False},
            ]
        }
        fake_client = FakeS3Client(["bucket-one"], object_responses)
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        listings = service.list_buckets_with_objects(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            max_keys=1,
        )

        bucket = listings[0]
        self.assertEqual(1, len(bucket.pages))
        self.assertEqual(["a.txt"], bucket.pages[0].keys)
        self.assertTrue(bucket.has_more)
        self.assertEqual("token-1", bucket.continuation_token)
        self.assertEqual([("bucket-one", None)], fake_client.list_objects_calls)

    def test_includes_common_prefixes_and_passes_prefix(self):
        object_responses = {
            "bucket-one": [
                {
                    "Contents": [],
                    "CommonPrefixes": [{"Prefix": "folder/"}],
                    "IsTruncated": False,
                }
            ]
        }
        fake_client = FakeS3Client(["bucket-one"], object_responses)
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        listing = service.list_objects_for_bucket(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            prefix="folder/",
        )

        self.assertEqual("folder/", listing.prefix)
        self.assertEqual(["folder/"], listing.pages[0].prefixes)
        self.assertEqual("/", fake_client.list_objects_kwargs[0]["Delimiter"])
        self.assertEqual("folder/", fake_client.list_objects_kwargs[0]["Prefix"])

    def test_supports_followup_listing_with_continuation_token(self):
        object_responses = {
            "bucket-one": [
                {"Contents": [{"Key": "b.txt"}], "IsTruncated": False},
            ]
        }
        fake_client = FakeS3Client(["bucket-one"], object_responses)
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        listing = service.list_objects_for_bucket(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            continuation_token="token-1",
        )

        self.assertEqual(["b.txt"], listing.pages[0].keys)
        self.assertFalse(listing.has_more)
        self.assertIsNone(listing.continuation_token)
        self.assertEqual("token-1", fake_client.list_objects_kwargs[0]["ContinuationToken"])

    def test_get_object_details_returns_metadata(self):
        last_modified = datetime(2024, 1, 1, 12, 0, 0)
        head_responses = {
            ("bucket-one", "a.txt"): {
                "ContentLength": 123,
                "LastModified": last_modified,
                "StorageClass": "STANDARD",
                "ETag": '"abc123"',
                "ContentType": "text/plain",
                "Metadata": {"custom": "value"},
            }
        }
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]}, head_responses)
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        details = service.get_object_details(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="a.txt",
        )

        self.assertEqual(123, details.size)
        self.assertEqual(last_modified, details.last_modified)
        self.assertEqual("STANDARD", details.storage_class)
        self.assertEqual('"abc123"', details.etag)
        self.assertEqual("text/plain", details.content_type)
        self.assertEqual({"custom": "value"}, details.metadata)
        self.assertEqual(1, len(fake_client.head_object_calls))
        self.assertEqual("bucket-one", fake_client.head_object_calls[0]["Bucket"])
        self.assertEqual("a.txt", fake_client.head_object_calls[0]["Key"])

    def test_download_object_saves_to_destination(self):
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        service.download_object(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="a.txt",
            destination="/tmp/a.txt",
        )

        self.assertEqual([("bucket-one", "a.txt", "/tmp/a.txt")], fake_client.download_file_calls)

    def test_download_object_reports_progress_and_supports_cancel(self):
        transfer_sequences = {("download", "bucket-one", "a.txt"): [1024, 2048, 1024]}
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            transfer_sequences=transfer_sequences,
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        reported = []
        cancel_flag = {"value": False}

        def progress(total):
            reported.append(total)
            if len(reported) == 2:
                cancel_flag["value"] = True

        def cancel_requested():
            return cancel_flag["value"]

        with self.assertRaises(TransferCancelledError):
            service.download_object(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="a.txt",
                destination="/tmp/a.txt",
                progress_callback=progress,
                cancel_requested=cancel_requested,
            )

        self.assertEqual([1024, 3072], reported)

    def test_upload_object_sends_source_file(self):
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        service.upload_object(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="folder/a.txt",
            source_path="/tmp/local.txt",
        )

        self.assertEqual(
            [("/tmp/local.txt", "bucket-one", "folder/a.txt")],
            fake_client.upload_file_calls,
        )
        self.assertIsNotNone(fake_client.upload_file_configs[0])

    def test_upload_object_reports_progress(self):
        transfer_sequences = {("upload", "bucket-one", "folder/a.txt"): [512, 512, 256]}
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            transfer_sequences=transfer_sequences,
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        reported = []

        service.upload_object(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="folder/a.txt",
            source_path="/tmp/local.txt",
            progress_callback=lambda total: reported.append(total),
        )

        self.assertEqual([512, 1024, 1280], reported)

    def test_upload_object_supports_cancel(self):
        transfer_sequences = {("upload", "bucket-one", "folder/a.txt"): [512, 512]}
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            transfer_sequences=transfer_sequences,
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        cancel_flag = {"value": False}

        def cancel_requested():
            return cancel_flag["value"]

        def progress(total):
            cancel_flag["value"] = True

        with self.assertRaises(TransferCancelledError):
            service.upload_object(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="folder/a.txt",
                source_path="/tmp/local.txt",
                progress_callback=progress,
                cancel_requested=cancel_requested,
            )

    def test_upload_object_passes_transfer_config(self):
        class FakeTransferConfig:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        original_config = services.TransferConfig
        services.TransferConfig = FakeTransferConfig
        try:
            service.upload_object(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="folder/a.txt",
                source_path="/tmp/local.txt",
                multipart_threshold=1024,
                multipart_chunk_size=2048,
                max_concurrency=3,
            )
        finally:
            services.TransferConfig = original_config

        config = fake_client.upload_file_configs[0]
        self.assertEqual(
            {
                "multipart_threshold": 1024,
                "multipart_chunksize": 2048,
                "max_concurrency": 3,
            },
            config.kwargs,
        )

    def test_upload_object_sanitizes_transfer_config(self):
        class FakeTransferConfig:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        original_config = services.TransferConfig
        services.TransferConfig = FakeTransferConfig
        try:
            service.upload_object(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="folder/a.txt",
                source_path="/tmp/local.txt",
                multipart_threshold=0,
                multipart_chunk_size=-5,
                max_concurrency=0,
            )
        finally:
            services.TransferConfig = original_config

        config = fake_client.upload_file_configs[0]
        self.assertEqual(
            {
                "multipart_threshold": services.DEFAULT_MULTIPART_THRESHOLD,
                "multipart_chunksize": services.DEFAULT_MULTIPART_CHUNK_SIZE,
                "max_concurrency": services.DEFAULT_MAX_CONCURRENCY,
            },
            config.kwargs,
        )

    def test_delete_object_removes_target_file(self):
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        service.delete_object(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="folder/a.txt",
        )

        self.assertEqual(
            [("bucket-one", "folder/a.txt")],
            fake_client.delete_object_calls,
        )

    def test_generate_presigned_get_url_passes_response_headers(self):
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            presigned_url_outputs={
                ("get_object", "bucket-one", "file.txt"): "https://example.com/get",
            },
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        url = service.generate_presigned_url(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="file.txt",
            method="get",
            expires_in=600,
            content_type="text/plain",
            content_disposition="attachment"
            
        )

        self.assertEqual("https://example.com/get", url)
        self.assertEqual(1, len(fake_client.presigned_url_calls))
        call = fake_client.presigned_url_calls[0]
        self.assertEqual("get_object", call["method"])
        self.assertEqual(600, call["expires_in"])
        self.assertEqual("bucket-one", call["params"]["Bucket"])
        self.assertEqual("file.txt", call["params"]["Key"])
        self.assertEqual("text/plain", call["params"]["ResponseContentType"])
        self.assertEqual("attachment", call["params"]["ResponseContentDisposition"])

    def test_generate_presigned_put_url_uses_put_object_operation(self):
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            presigned_url_outputs={
                ("put_object", "bucket-one", "upload.bin"): "https://example.com/put",
            },
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        url = service.generate_presigned_url(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="upload.bin",
            method="put",
            expires_in=120,
            content_type="application/octet-stream",
            content_disposition="inline",
        )

        self.assertEqual("https://example.com/put", url)
        call = fake_client.presigned_url_calls[0]
        self.assertEqual("put_object", call["method"])
        self.assertEqual("application/octet-stream", call["params"]["ContentType"])
        self.assertEqual("inline", call["params"]["ContentDisposition"])

    def test_generate_presigned_post_builds_prefix_conditions(self):
        fake_client = FakeS3Client(
            ["bucket-one"],
            {"bucket-one": [{"Contents": []}]},
            presigned_post_outputs={
                ("bucket-one", "uploads/${filename}"): {
                    "url": "https://example.com/post",
                    "fields": {"key": "uploads/${filename}"},
                },
            },
        )
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        result = service.generate_presigned_url(
            endpoint_url="https://example.com",
            access_key="access",
            secret_key="secret",
            bucket_name="bucket-one",
            key="uploads",
            method="post",
            expires_in=900,
            content_type="text/plain",
            post_key_mode="prefix",
            max_size=1024,
        )

        self.assertEqual("https://example.com/post", result["url"])
        self.assertEqual({"key": "uploads/${filename}"}, result["fields"])
        call = fake_client.presigned_post_calls[0]
        self.assertEqual("bucket-one", call["bucket"])
        self.assertEqual("uploads/${filename}", call["key"])
        self.assertEqual(900, call["expires_in"])
        self.assertIn(["content-length-range", 0, 1024], call["conditions"])
        self.assertIn(["starts-with", "$key", "uploads/"], call["conditions"])
        self.assertIn(["eq", "$Content-Type", "text/plain"], call["conditions"])
        self.assertEqual("uploads/${filename}", call["fields"]["key"])
        self.assertEqual("text/plain", call["fields"]["Content-Type"])

    def test_generate_presigned_url_validates_inputs(self):
        fake_client = FakeS3Client(["bucket-one"], {"bucket-one": [{"Contents": []}]})
        service = S3BrowserService(client_factory=lambda *_, **__: fake_client)

        with self.assertRaises(ValueError):
            service.generate_presigned_url(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="file.txt",
                method="delete",
            )

        with self.assertRaises(ValueError):
            service.generate_presigned_url(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="file.txt",
                method="get",
                expires_in=0,
            )

        with self.assertRaises(ValueError):
            service.generate_presigned_url(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="file.txt",
                method="post",
                post_key_mode="single",
                max_size=0,
            )

        with self.assertRaises(ValueError):
            service.generate_presigned_url(
                endpoint_url="https://example.com",
                access_key="access",
                secret_key="secret",
                bucket_name="bucket-one",
                key="file.txt",
                method="post",
                post_key_mode="unknown",
                max_size=10,
            )


if __name__ == "__main__":
    unittest.main()
