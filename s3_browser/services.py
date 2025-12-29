from __future__ import annotations
"""Business logic for interacting with S3."""
from typing import Callable, Optional

try:  # pragma: no cover - optional dependency for tests
    import boto3
except ModuleNotFoundError:  # pragma: no cover - handled lazily
    boto3 = None
try:  # pragma: no cover - optional dependency for tests
    from botocore.client import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ModuleNotFoundError:  # pragma: no cover - lightweight fallbacks
    class Config:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            pass

    class BotoCoreError(Exception):  # type: ignore[no-redef]
        pass

    class ClientError(Exception):  # type: ignore[no-redef]
        pass

from .models import BucketListing, ObjectDetails, ObjectPage


class TransferCancelledError(RuntimeError):
    """Raised when an upload or download is cancelled by the caller."""


PAGE_SIZE = 50

class S3BrowserService:
    """Encapsulates S3 listing logic independent of any UI technology."""

    def __init__(self, client_factory: Callable[..., object] | None = None):
        if client_factory is not None:
            self._client_factory = client_factory
        else:
            if boto3 is None:  # pragma: no cover - depends on environment
                raise ModuleNotFoundError("boto3 is required to use S3BrowserService")
            self._client_factory = boto3.client

    def list_buckets_with_objects(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        max_keys: int = 10,
        prefix: str = "",
        delimiter: str | None = "/",
    ) -> list[BucketListing]:
        """Return bucket listings with paginated objects.

        Raises:
            BotoCoreError | ClientError: when unable to connect or list buckets.
        """
        client = self._create_client(endpoint_url, access_key, secret_key)
        return [
            self._build_bucket_listing(
                client,
                bucket_name,
                max_keys=max_keys,
                prefix=prefix,
                delimiter=delimiter,
            )
            for bucket_name in self.list_buckets(
                endpoint_url=endpoint_url,
                access_key=access_key,
                secret_key=secret_key,
                client=client,
            )
        ]

    def list_buckets(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        client=None,
    ) -> list[str]:
        """Return the available bucket names."""

        client = client or self._create_client(endpoint_url, access_key, secret_key)
        buckets_response = client.list_buckets()
        return [bucket["Name"] for bucket in buckets_response.get("Buckets", [])]

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
    ) -> BucketListing:
        """Return paginated objects for a single bucket."""

        client = self._create_client(endpoint_url, access_key, secret_key)
        return self._build_bucket_listing(
            client,
            bucket_name,
            max_keys=max_keys,
            prefix=prefix,
            delimiter=delimiter,
            continuation_token=continuation_token,
        )

    def _create_client(self, endpoint_url: str, access_key: str, secret_key: str):
        config = Config(signature_version="s3v4")
        return self._client_factory(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=config,
        )

    def _build_bucket_listing(
        self,
        client,
        bucket_name: str,
        *,
        max_keys: int,
        prefix: str = "",
        delimiter: str | None = "/",
        continuation_token: str | None = None,
    ) -> BucketListing:
        pages: list[ObjectPage] = []
        request_token = continuation_token
        next_continuation_token: str | None = None
        page_number = 1
        bucket_error: str | None = None
        remaining = max_keys
        has_more = False

        while remaining > 0:
            list_params = {"Bucket": bucket_name, "MaxKeys": min(remaining, PAGE_SIZE)}
            if prefix:
                list_params["Prefix"] = prefix
            if delimiter:
                list_params["Delimiter"] = delimiter
            if request_token:
                list_params["ContinuationToken"] = request_token

            try:
                obj_response = client.list_objects_v2(**list_params)
                keys = [obj["Key"] for obj in obj_response.get("Contents", [])]
                prefixes = [common["Prefix"] for common in obj_response.get("CommonPrefixes", [])]
                pages.append(ObjectPage(number=page_number, keys=keys, prefixes=prefixes))

                remaining -= len(keys) + len(prefixes)
                truncated = obj_response.get("IsTruncated", False)
                response_token = obj_response.get("NextContinuationToken")

                if not keys and not prefixes:
                    if truncated and response_token:
                        request_token = response_token
                        page_number += 1
                        continue
                    break

                if truncated and remaining > 0:
                    request_token = response_token
                    page_number += 1
                    continue

                if truncated and response_token:
                    next_continuation_token = response_token
                    has_more = True
                break
            except (ClientError, BotoCoreError) as exc:  # pragma: no cover - passthrough
                pages.append(ObjectPage(number=page_number, keys=[], error=str(exc)))
                bucket_error = str(exc)
                break

        return BucketListing(
            name=bucket_name,
            prefix=prefix or "",
            delimiter=delimiter or "",
            pages=pages,
            error=bucket_error,
            has_more=has_more,
            continuation_token=next_continuation_token,
        )

    def get_object_details(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
    ) -> ObjectDetails:
        """Fetch metadata about a single object."""

        client = self._create_client(endpoint_url, access_key, secret_key)
        response = client.head_object(Bucket=bucket_name, Key=key, ChecksumMode='ENABLED')
        # print("head response:", response)
        checksums = {
            "CRC32": response.get("ChecksumCRC32"),
            "CRC32C": response.get("ChecksumCRC32C"),
            "SHA1": response.get("ChecksumSHA1"),
            "SHA256": response.get("ChecksumSHA256"),
        }
        checksums = {name: value for name, value in checksums.items() if value}
        return ObjectDetails(
            bucket=bucket_name,
            key=key,
            size=response.get("ContentLength"),
            last_modified=response.get("LastModified"),
            storage_class=response.get("StorageClass"),
            etag=response.get("ETag"),
            content_type=response.get("ContentType"),
            metadata=dict(response.get("Metadata") or {}),
            checksums=checksums,
        )

    def download_object(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
        destination: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Download an S3 object to the provided destination path."""

        client = self._create_client(endpoint_url, access_key, secret_key)
        callback = self._build_transfer_callback(progress_callback, cancel_requested)
        client.download_file(bucket_name, key, destination, Callback=callback)

    def upload_object(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
        source_path: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Upload a local file to the target bucket/key."""

        client = self._create_client(endpoint_url, access_key, secret_key)
        callback = self._build_transfer_callback(progress_callback, cancel_requested)
        client.upload_file(
            source_path,
            bucket_name,
            key,
            Callback=callback,
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
            ExtraArgs={"ChecksumAlgorithm": "SHA256",
                        "Metadata": {"pys3b_upload": "true"},
                    },
        )

    def delete_object(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
    ) -> None:
        """Delete an object from the target bucket/key."""

        client = self._create_client(endpoint_url, access_key, secret_key)
        client.delete_object(Bucket=bucket_name, Key=key)

    def generate_presigned_url(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        key: str,
        method: str = "get",
        expires_in: int = 3600,
        content_type: str | None = None,
        content_disposition: str | None = None,
    ) -> str:
        """Create a presigned URL for the requested object operation."""

        operation = method.strip().lower()
        if operation not in {"get", "put"}:
            raise ValueError("method must be either 'get' or 'put'")
        if expires_in <= 0:
            raise ValueError("expires_in must be greater than zero")

        client_method = "get_object" if operation == "get" else "put_object"
        params: dict[str, str] = {"Bucket": bucket_name, "Key": key}
        if operation == "get":
            if content_type:
                params["ResponseContentType"] = content_type
            if content_disposition:
                params["ResponseContentDisposition"] = content_disposition
        else:
            if content_type:
                params["ContentType"] = content_type
            if content_disposition:
                params["ContentDisposition"] = content_disposition

        client = self._create_client(endpoint_url, access_key, secret_key)
        return client.generate_presigned_url(
            client_method,
            Params=params,
            ExpiresIn=expires_in,
        )

    def _build_transfer_callback(
        self,
        progress_callback: Optional[Callable[[int], None]],
        cancel_requested: Optional[Callable[[], bool]],
    ):
        if not progress_callback and not cancel_requested:
            return None

        transferred = 0

        def _callback(bytes_amount: int) -> None:
            nonlocal transferred
            if cancel_requested and cancel_requested():
                raise TransferCancelledError("Transfer cancelled by user")
            transferred += bytes_amount
            if progress_callback:
                progress_callback(transferred)
            if cancel_requested and cancel_requested():
                raise TransferCancelledError("Transfer cancelled by user")

        return _callback
