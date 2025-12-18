from __future__ import annotations
"""Application settings persistence helpers."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path


@dataclass
class AppSettings:
    """Simple container for persistent app settings."""

    fetch_limit: int = 10


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
        try:
            fetch_value = int(fetch_limit)
        except (TypeError, ValueError):
            fetch_value = AppSettings.fetch_limit
        if fetch_value <= 0:
            fetch_value = AppSettings.fetch_limit
        return AppSettings(fetch_limit=fetch_value)

    def save(self, settings: AppSettings) -> None:
        payload = {"fetch_limit": max(int(settings.fetch_limit), 1)}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except OSError:
            # Persist best-effort; ignore filesystem issues.
            return
