from __future__ import annotations
"""View-agnostic presenter that wraps controller operations."""
from dataclasses import replace
import logging
import threading
from typing import Callable, Iterable

from botocore.exceptions import BotoCoreError, ClientError

from .controller import S3BrowserController
from .models import BucketListing, ObjectDetails
from .profiles import ConnectionProfile
from .services import TransferCancelledError
from .settings import AppSettings, SettingsStorage
from .ui_utils import PackageInfo, load_package_info


DispatchFn = Callable[[Callable[[], None]], None]
SuccessFn = Callable[[object], None]
ErrorFn = Callable[[str], None]
DoneFn = Callable[[], None]

LOGGER = logging.getLogger(__name__)


def _format_error(exc: Exception) -> str:
    return str(exc)


class S3BrowserPresenter:
    """Runs background operations and returns results via callbacks."""

    def __init__(
        self,
        *,
        controller: S3BrowserController | None = None,
        settings_storage: SettingsStorage | None = None,
        dispatch: DispatchFn | None = None,
    ) -> None:
        self._controller = controller or S3BrowserController()
        self._settings_storage = settings_storage or SettingsStorage()
        self._settings = self._settings_storage.load()
        self._dispatch = dispatch or (lambda func: func())
        self._package_info = load_package_info()

    @property
    def settings(self) -> AppSettings:
        return replace(self._settings)

    @property
    def package_info(self) -> PackageInfo:
        return self._package_info

    @property
    def is_connected(self) -> bool:
        return self._controller.is_connected

    @property
    def selected_profile(self) -> str | None:
        return self._controller.selected_profile

    def save_settings(self, settings: AppSettings) -> None:
        self._settings = settings
        self._settings_storage.save(settings)

    def update_fetch_limit(self, value: int) -> None:
        normalized = max(int(value), 1)
        self._settings = replace(self._settings, fetch_limit=normalized)
        self._settings_storage.save(self._settings)

    def update_last_connection(self, connection: str) -> None:
        if not self._settings.remember_last_bucket:
            return
        self._settings = replace(self._settings, last_connection=connection or "")
        self._settings_storage.save(self._settings)

    def update_last_bucket(self, bucket: str) -> None:
        if not self._settings.remember_last_bucket:
            return
        self._settings = replace(self._settings, last_bucket=bucket or "")
        self._settings_storage.save(self._settings)

    def list_profiles(self) -> list[ConnectionProfile]:
        return self._controller.list_profiles()

    def save_profile(self, profile: ConnectionProfile, *, original_name: str | None = None) -> None:
        self._controller.save_profile(profile, original_name=original_name)

    def delete_profile(self, name: str) -> None:
        self._controller.delete_profile(name)

    def get_profile(self, name: str) -> ConnectionProfile:
        return self._controller.get_profile(name)

    def maybe_auto_connect_profile(self) -> str | None:
        if not self._settings.remember_last_bucket:
            return None
        return self._settings.last_connection or None

    def connect(
        self,
        *,
        profile_name: str,
        on_success: Callable[[list[str]], None],
        on_error: ErrorFn,
        on_done: DoneFn | None = None,
    ) -> None:
        LOGGER.debug("Connecting using profile '%s'", profile_name)
        def task() -> None:
            try:
                buckets = self._controller.connect_with_profile(profile_name)
            except (BotoCoreError, ClientError) as exc:
                LOGGER.exception("Connection error for profile '%s'", profile_name)
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                LOGGER.exception("Unexpected connection error for profile '%s'", profile_name)
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                LOGGER.debug("Connected using profile '%s' (%d buckets)", profile_name, len(buckets))
                self._dispatch(lambda: on_success(buckets))
            finally:
                if on_done:
                    self._dispatch(on_done)

        threading.Thread(target=task, daemon=True).start()

    def refresh_buckets(
        self,
        *,
        on_success: Callable[[list[str]], None],
        on_error: ErrorFn,
        on_done: DoneFn | None = None,
    ) -> None:
        LOGGER.debug("Refreshing buckets")
        def task() -> None:
            try:
                buckets = self._controller.refresh_buckets()
            except (BotoCoreError, ClientError) as exc:
                LOGGER.exception("Bucket refresh error")
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                LOGGER.exception("Unexpected bucket refresh error")
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                LOGGER.debug("Bucket refresh returned %d bucket(s)", len(buckets))
                self._dispatch(lambda: on_success(buckets))
            finally:
                if on_done:
                    self._dispatch(on_done)

        threading.Thread(target=task, daemon=True).start()

    def list_objects(
        self,
        *,
        bucket_name: str,
        max_keys: int,
        prefix: str = "",
        delimiter: str | None = "/",
        continuation_token: str | None = None,
        on_success: Callable[[BucketListing], None],
        on_error: ErrorFn,
        on_done: DoneFn | None = None,
    ) -> None:
        LOGGER.debug("Listing objects for bucket '%s'", bucket_name)
        def task() -> None:
            try:
                listing = self._controller.list_objects(
                    bucket_name=bucket_name,
                    max_keys=max_keys,
                    prefix=prefix,
                    delimiter=delimiter,
                    continuation_token=continuation_token,
                )
            except (BotoCoreError, ClientError) as exc:
                LOGGER.exception("List objects error for bucket '%s'", bucket_name)
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                LOGGER.exception("Unexpected list objects error for bucket '%s'", bucket_name)
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                LOGGER.debug(
                    "Listed %d page(s) for bucket '%s'",
                    len(listing.pages),
                    bucket_name,
                )
                self._dispatch(lambda: on_success(listing))
            finally:
                if on_done:
                    self._dispatch(on_done)

        threading.Thread(target=task, daemon=True).start()

    def get_object_details(
        self,
        *,
        bucket_name: str,
        key: str,
        on_success: Callable[[ObjectDetails], None],
        on_error: ErrorFn,
    ) -> None:
        def task() -> None:
            try:
                details = self._controller.get_object_details(bucket_name=bucket_name, key=key)
            except (BotoCoreError, ClientError) as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                self._dispatch(lambda: on_success(details))

        threading.Thread(target=task, daemon=True).start()

    def delete_object(
        self,
        *,
        bucket_name: str,
        key: str,
        on_success: DoneFn,
        on_error: ErrorFn,
    ) -> None:
        def task() -> None:
            try:
                self._controller.delete_object(bucket_name=bucket_name, key=key)
            except (BotoCoreError, ClientError) as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                self._dispatch(on_success)

        threading.Thread(target=task, daemon=True).start()

    def download_object(
        self,
        *,
        bucket_name: str,
        key: str,
        destination: str,
        on_progress: Callable[[int], None] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        on_success: DoneFn | None = None,
        on_error: ErrorFn | None = None,
        on_cancelled: ErrorFn | None = None,
        on_done: DoneFn | None = None,
    ) -> None:
        progress_callback = None
        if on_progress:
            progress_callback = lambda total: self._dispatch(lambda: on_progress(total))

        def task() -> None:
            try:
                self._controller.download_object(
                    bucket_name=bucket_name,
                    key=key,
                    destination=destination,
                    progress_callback=progress_callback,
                    cancel_requested=cancel_requested,
                )
            except TransferCancelledError as exc:
                if on_cancelled:
                    self._dispatch(lambda: on_cancelled(_format_error(exc)))
            except (BotoCoreError, ClientError) as exc:
                if on_error:
                    self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                if on_error:
                    self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                if on_success:
                    self._dispatch(on_success)
            finally:
                if on_done:
                    self._dispatch(on_done)

        threading.Thread(target=task, daemon=True).start()

    def upload_object(
        self,
        *,
        bucket_name: str,
        key: str,
        source_path: str,
        multipart_threshold: int | None = None,
        multipart_chunk_size: int | None = None,
        max_concurrency: int | None = None,
        on_progress: Callable[[int], None] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        on_success: DoneFn | None = None,
        on_error: ErrorFn | None = None,
        on_cancelled: ErrorFn | None = None,
        on_done: DoneFn | None = None,
    ) -> None:
        progress_callback = None
        if on_progress:
            progress_callback = lambda total: self._dispatch(lambda: on_progress(total))

        def task() -> None:
            try:
                self._controller.upload_object(
                    bucket_name=bucket_name,
                    key=key,
                    source_path=source_path,
                    multipart_threshold=multipart_threshold,
                    multipart_chunk_size=multipart_chunk_size,
                    max_concurrency=max_concurrency,
                    progress_callback=progress_callback,
                    cancel_requested=cancel_requested,
                )
            except TransferCancelledError as exc:
                if on_cancelled:
                    self._dispatch(lambda: on_cancelled(_format_error(exc)))
            except (BotoCoreError, ClientError) as exc:
                if on_error:
                    self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                if on_error:
                    self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                if on_success:
                    self._dispatch(on_success)
            finally:
                if on_done:
                    self._dispatch(on_done)

        threading.Thread(target=task, daemon=True).start()

    def generate_presigned_url(
        self,
        *,
        bucket_name: str,
        key: str,
        method: str,
        expires_in: int,
        content_type: str | None,
        content_disposition: str | None,
        post_key_mode: str,
        max_size: int | None,
        on_success: Callable[[str | dict[str, dict[str, str] | str]], None],
        on_error: ErrorFn,
    ) -> None:
        def task() -> None:
            try:
                result = self._controller.generate_presigned_url(
                    bucket_name=bucket_name,
                    key=key,
                    method=method,
                    expires_in=expires_in,
                    content_type=content_type,
                    content_disposition=content_disposition,
                    post_key_mode=post_key_mode,
                    max_size=max_size,
                )
            except (BotoCoreError, ClientError) as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            except Exception as exc:
                self._dispatch(lambda: on_error(_format_error(exc)))
            else:
                self._dispatch(lambda: on_success(result))

        threading.Thread(target=task, daemon=True).start()

    def connect_with_profile_names(self, profiles: Iterable[ConnectionProfile]) -> list[str]:
        return [profile.name for profile in profiles]
