from __future__ import annotations
"""Application settings persistence helpers."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path


@dataclass
class AppSettings:
    """Simple container for persistent app settings."""

    fetch_limit: int = 10
    default_post_max_size: int = 10485760
    upload_multipart_threshold: int = 8 * 1024 * 1024
    upload_chunk_size: int = 8 * 1024 * 1024
    upload_max_concurrency: int = 10
    remember_last_bucket: bool = False
    last_bucket: str = ""
    last_connection: str = ""


class SettingsStorage:
    """JSON-backed persistence for :class:`AppSettings`."""

    def __init__(self, storage_path: str | Path | None = None):
        if storage_path is None:
            storage_path = Path.home() / ".pys3b_settings.json"
        self._path = Path(storage_path)

    def load(self) -> AppSettings:
        if not self._path.exists():
            return AppSettings()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return AppSettings()
        fetch_limit = data.get("fetch_limit", AppSettings.fetch_limit)
        default_post_max_size = data.get("default_post_max_size", AppSettings.default_post_max_size)
        upload_multipart_threshold = data.get(
            "upload_multipart_threshold",
            AppSettings.upload_multipart_threshold,
        )
        upload_chunk_size = data.get("upload_chunk_size", AppSettings.upload_chunk_size)
        upload_max_concurrency = data.get("upload_max_concurrency", AppSettings.upload_max_concurrency)
        try:
            fetch_value = int(fetch_limit)
        except (TypeError, ValueError):
            fetch_value = AppSettings.fetch_limit
        if fetch_value <= 0:
            fetch_value = AppSettings.fetch_limit
        try:
            max_size_value = int(default_post_max_size)
        except (TypeError, ValueError):
            max_size_value = AppSettings.default_post_max_size
        if max_size_value <= 0:
            max_size_value = AppSettings.default_post_max_size
        try:
            multipart_threshold_value = int(upload_multipart_threshold)
        except (TypeError, ValueError):
            multipart_threshold_value = AppSettings.upload_multipart_threshold
        if multipart_threshold_value <= 0:
            multipart_threshold_value = AppSettings.upload_multipart_threshold
        try:
            chunk_size_value = int(upload_chunk_size)
        except (TypeError, ValueError):
            chunk_size_value = AppSettings.upload_chunk_size
        if chunk_size_value <= 0:
            chunk_size_value = AppSettings.upload_chunk_size
        try:
            max_concurrency_value = int(upload_max_concurrency)
        except (TypeError, ValueError):
            max_concurrency_value = AppSettings.upload_max_concurrency
        if max_concurrency_value <= 0:
            max_concurrency_value = AppSettings.upload_max_concurrency
        remember_last_bucket = bool(data.get("remember_last_bucket", AppSettings.remember_last_bucket))
        last_bucket = data.get("last_bucket", AppSettings.last_bucket)
        if not isinstance(last_bucket, str):
            last_bucket = AppSettings.last_bucket
        last_connection = data.get("last_connection", AppSettings.last_connection)
        if not isinstance(last_connection, str):
            last_connection = AppSettings.last_connection
        return AppSettings(
            fetch_limit=fetch_value,
            default_post_max_size=max_size_value,
            upload_multipart_threshold=multipart_threshold_value,
            upload_chunk_size=chunk_size_value,
            upload_max_concurrency=max_concurrency_value,
            remember_last_bucket=remember_last_bucket,
            last_bucket=last_bucket,
            last_connection=last_connection,
        )

    def save(self, settings: AppSettings) -> None:
        payload = {
            "fetch_limit": max(int(settings.fetch_limit), 1),
            "default_post_max_size": max(int(settings.default_post_max_size), 1),
            "upload_multipart_threshold": max(int(settings.upload_multipart_threshold), 1),
            "upload_chunk_size": max(int(settings.upload_chunk_size), 1),
            "upload_max_concurrency": max(int(settings.upload_max_concurrency), 1),
            "remember_last_bucket": bool(settings.remember_last_bucket),
            "last_bucket": settings.last_bucket or "",
            "last_connection": settings.last_connection or "",
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except OSError:
            # Persist best-effort; ignore filesystem issues.
            return
