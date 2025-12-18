from __future__ import annotations
"""Connection profile models and persistence."""
from dataclasses import dataclass
import json
from pathlib import Path

import keyring
from keyring.errors import KeyringError


@dataclass
class ConnectionProfile:
    """Represents a saved S3 connection."""

    name: str
    endpoint_url: str
    access_key: str
    secret_key: str


class KeychainStore:
    """Encapsulates OS keychain access for secrets."""

    def __init__(self, service_name: str = "pys3b"):
        self._service_name = service_name

    def get_secret(self, profile_name: str) -> str:
        if not profile_name:
            return ""
        try:
            return keyring.get_password(self._service_name, profile_name) or ""
        except KeyringError:
            return ""

    def set_secret(self, profile_name: str, secret_key: str) -> None:
        if not profile_name:
            return
        if not secret_key:
            self.delete_secret(profile_name)
            return
        try:
            keyring.set_password(self._service_name, profile_name, secret_key)
        except KeyringError:
            return

    def delete_secret(self, profile_name: str) -> None:
        if not profile_name:
            return
        try:
            keyring.delete_password(self._service_name, profile_name)
        except KeyringError:
            return


class ProfileStorage:
    """Simple JSON-backed store for connection profiles."""

    def __init__(self, storage_path: str | Path | None = None):
        if storage_path is None:
            storage_path = Path.home() / ".pys3b_connections.json"
        self._path = Path(storage_path)
        self._keychain = KeychainStore()

    def load(self) -> list[ConnectionProfile]:
        if not self._path.exists():
            return []
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

        profiles: list[ConnectionProfile] = []
        sanitized: list[dict[str, str]] = []
        saw_plaintext = False
        for entry in data:
            try:
                name = entry["name"]
                endpoint_url = entry["endpoint_url"]
                access_key = entry["access_key"]
                secret_key = entry.get("secret_key", "")
                if secret_key:
                    saw_plaintext = True
                    self._keychain.set_secret(name, secret_key)
                else:
                    secret_key = self._keychain.get_secret(name)
                profiles.append(
                    ConnectionProfile(
                        name=name,
                        endpoint_url=endpoint_url,
                        access_key=access_key,
                        secret_key=secret_key,
                    )
                )
                sanitized.append(
                    {
                        "name": name,
                        "endpoint_url": endpoint_url,
                        "access_key": access_key,
                    }
                )
            except KeyError:
                continue
        if saw_plaintext:
            self._write_data(sanitized)
        return profiles

    def save(self, profiles: list[ConnectionProfile]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = []
        for profile in profiles:
            self._keychain.set_secret(profile.name, profile.secret_key)
            data.append(
                {
                    "name": profile.name,
                    "endpoint_url": profile.endpoint_url,
                    "access_key": profile.access_key,
                }
            )
        existing_names = self._load_profile_names()
        current_names = {profile.name for profile in profiles}
        for name in existing_names - current_names:
            self._keychain.delete_secret(name)
        self._write_data(data)

    def _load_profile_names(self) -> set[str]:
        if not self._path.exists():
            return set()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return set()
        names = set()
        for entry in data:
            name = entry.get("name")
            if isinstance(name, str) and name:
                names.add(name)
        return names

    def _write_data(self, data: list[dict[str, str]]) -> None:
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")
