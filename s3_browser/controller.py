from __future__ import annotations
"""Controller layer for the Tkinter S3 browser view."""

from typing import Callable, Optional

from .models import BucketListing, ObjectDetails
from .profiles import ConnectionProfile, ProfileStorage
from .services import S3BrowserService


class NotConnectedError(RuntimeError):
    """Raised when an S3 operation is attempted before connecting."""


class S3BrowserController:
    """Coordinates user actions with the :class:`S3BrowserService`."""

    def __init__(
        self,
        service: S3BrowserService | None = None,
        storage: ProfileStorage | None = None,
    ):
        self._service = service or S3BrowserService()
        self._storage = storage or ProfileStorage()
        self._connection_params: dict[str, str] | None = None
        self._profiles: list[ConnectionProfile] = self._storage.load()
        self._selected_profile: str | None = None

    @property
    def is_connected(self) -> bool:
        return self._connection_params is not None

    @property
    def selected_profile(self) -> str | None:
        return self._selected_profile

    def list_profiles(self) -> list[ConnectionProfile]:
        return list(self._profiles)

    def save_profile(self, profile: ConnectionProfile, *, original_name: str | None = None) -> None:
        if original_name and original_name != profile.name:
            self._profiles = [p for p in self._profiles if p.name != original_name]
        self._upsert_profile(profile)
        self._persist_profiles()

    def delete_profile(self, name: str) -> None:
        before = len(self._profiles)
        self._profiles = [p for p in self._profiles if p.name != name]
        if len(self._profiles) == before:
            raise ValueError(f"Profile '{name}' does not exist")
        if self._selected_profile == name:
            self._selected_profile = None
        self._persist_profiles()

    def get_profile(self, name: str) -> ConnectionProfile:
        for profile in self._profiles:
            if profile.name == name:
                return profile
        raise ValueError(f"Profile '{name}' does not exist")

    def connect_with_profile(self, name: str) -> list[str]:
        profile = self.get_profile(name)
        self._selected_profile = name
        return self.connect(
            endpoint_url=profile.endpoint_url,
            access_key=profile.access_key,
            secret_key=profile.secret_key,
        )

    def connect(self, *, endpoint_url: str, access_key: str, secret_key: str) -> list[str]:
        connection_params = {
            "endpoint_url": endpoint_url,
            "access_key": access_key,
            "secret_key": secret_key,
        }
        buckets = self._service.list_buckets(**connection_params)
        self._connection_params = connection_params
        return buckets

    def refresh_buckets(self) -> list[str]:
        params = self._require_connection()
        return self._service.list_buckets(**params)

    def list_objects(
        self,
        *,
        bucket_name: str,
        max_keys: int = 10,
        prefix: str = "",
        delimiter: str | None = "/",
        continuation_token: str | None = None,
    ) -> BucketListing:
        params = self._require_connection()
        return self._service.list_objects_for_bucket(
            bucket_name=bucket_name,
            max_keys=max_keys,
            prefix=prefix,
            delimiter=delimiter,
            continuation_token=continuation_token,
            **params,
        )

    def get_object_details(self, *, bucket_name: str, key: str) -> ObjectDetails:
        params = self._require_connection()
        return self._service.get_object_details(
            bucket_name=bucket_name,
            key=key,
            **params,
        )

    def download_object(
        self,
        *,
        bucket_name: str,
        key: str,
        destination: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        params = self._require_connection()
        self._service.download_object(
            bucket_name=bucket_name,
            key=key,
            destination=destination,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
            **params,
        )

    def upload_object(
        self,
        *,
        bucket_name: str,
        key: str,
        source_path: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> None:
        params = self._require_connection()
        self._service.upload_object(
            bucket_name=bucket_name,
            key=key,
            source_path=source_path,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
            **params,
        )

    def delete_object(self, *, bucket_name: str, key: str) -> None:
        params = self._require_connection()
        self._service.delete_object(
            bucket_name=bucket_name,
            key=key,
            **params,
        )

    def generate_presigned_url(
        self,
        *,
        bucket_name: str,
        key: str,
        method: str = "get",
        expires_in: int = 3600,
        content_type: str | None = None,
        content_disposition: str | None = None,
        post_key_mode: str = "single",
        max_size: int | None = None,
    ) -> str | dict[str, dict[str, str] | str]:
        params = self._require_connection()
        return self._service.generate_presigned_url(
            bucket_name=bucket_name,
            key=key,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            content_disposition=content_disposition,
            post_key_mode=post_key_mode,
            max_size=max_size,
            **params,
        )

    def _require_connection(self) -> dict[str, str]:
        if not self._connection_params:
            raise NotConnectedError("Not connected to S3")
        return self._connection_params

    def _upsert_profile(self, profile: ConnectionProfile) -> None:
        for idx, existing in enumerate(self._profiles):
            if existing.name == profile.name:
                self._profiles[idx] = profile
                break
        else:
            self._profiles.append(profile)

    def _persist_profiles(self) -> None:
        self._storage.save(self._profiles)
