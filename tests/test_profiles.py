import json
import tempfile
import unittest
from pathlib import Path

from s3_browser.profiles import ConnectionProfile, ProfileStorage


class FakeKeychain:
    def __init__(self):
        self.secrets = {}
        self.set_calls = []
        self.delete_calls = []

    def get_secret(self, profile_name: str) -> str:
        return self.secrets.get(profile_name, "")

    def set_secret(self, profile_name: str, secret_key: str) -> None:
        self.set_calls.append((profile_name, secret_key))
        self.secrets[profile_name] = secret_key

    def delete_secret(self, profile_name: str) -> None:
        self.delete_calls.append(profile_name)
        self.secrets.pop(profile_name, None)


class ProfileStorageTests(unittest.TestCase):
    def test_load_migrates_plaintext_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connections.json"
            payload = [
                {
                    "name": "alpha",
                    "endpoint_url": "https://one",
                    "access_key": "a",
                    "secret_key": "secret",
                }
            ]
            path.write_text(json.dumps(payload), encoding="utf-8")
            storage = ProfileStorage(path)
            fake_keychain = FakeKeychain()
            storage._keychain = fake_keychain

            profiles = storage.load()

            self.assertEqual("secret", profiles[0].secret_key)
            self.assertEqual([("alpha", "secret")], fake_keychain.set_calls)
            sanitized = json.loads(path.read_text(encoding="utf-8"))
            self.assertNotIn("secret_key", sanitized[0])

    def test_load_uses_keychain_when_secret_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connections.json"
            payload = [
                {
                    "name": "alpha",
                    "endpoint_url": "https://one",
                    "access_key": "a",
                }
            ]
            path.write_text(json.dumps(payload), encoding="utf-8")
            storage = ProfileStorage(path)
            fake_keychain = FakeKeychain()
            fake_keychain.secrets["alpha"] = "stored-secret"
            storage._keychain = fake_keychain

            profiles = storage.load()

            self.assertEqual("stored-secret", profiles[0].secret_key)

    def test_save_deletes_removed_keychain_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "connections.json"
            payload = [
                {"name": "alpha", "endpoint_url": "https://one", "access_key": "a"},
                {"name": "beta", "endpoint_url": "https://two", "access_key": "b"},
            ]
            path.write_text(json.dumps(payload), encoding="utf-8")
            storage = ProfileStorage(path)
            fake_keychain = FakeKeychain()
            storage._keychain = fake_keychain

            profiles = [
                ConnectionProfile(name="alpha", endpoint_url="https://one", access_key="a", secret_key="secret"),
            ]
            storage.save(profiles)

            self.assertEqual(["beta"], fake_keychain.delete_calls)
            self.assertEqual([("alpha", "secret")], fake_keychain.set_calls)


if __name__ == "__main__":
    unittest.main()
