# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Commands

```bash
# Install for development
pip install -e '.[dev]'

# Run the application
pys3b
pys3b --verbose   # with debug logging

# Run tests
pytest
pytest tests/test_controller.py                                          # single file
pytest tests/test_controller.py::TestS3BrowserController::test_name     # single test
pytest -v                                                                # verbose
```

## Architecture

**pys3b** is a PySide6 desktop app for browsing/managing AWS S3 buckets. It follows a layered MVC-like architecture:

```
qt_view.py  →  presenter.py  →  controller.py  →  services.py  →  boto3
    UI            async           orchestration      S3 ops       AWS SDK
```

**`models.py`** — Pure data classes: `ObjectPage`, `BucketListing`, `ObjectDetails`.

**`services.py`** — `S3BrowserService` wraps a boto3 S3 client for all S3 operations (list, download, upload with progress callbacks, delete, signed URLs). Designed for dependency injection in tests.

**`controller.py`** — `S3BrowserController` coordinates profiles/credentials and delegates to `S3BrowserService`. Enforces connection state before any S3 operation.

**`presenter.py`** — `S3BrowserPresenter` runs background S3 operations and delivers results via callbacks. Uses a dispatcher function for thread-safe UI updates. This is the boundary between background threads and the UI.

**`qt_view.py`** — `S3BrowserWindow` (main window) and `UploadDropTreeView` (drag-drop upload). Calls presenter methods and receives results via callbacks dispatched to the Qt main thread.

**`profiles.py`** — `ProfileStorage` persists connection profiles to `~/.pys3b_connections.json`. `KeychainStore` integrates with the OS keychain (via `keyring`) for secret key storage.

**`settings.py`** — `AppSettings` is the data class for persistent settings. `SettingsStorage` reads/writes it to `~/.pys3b_settings.json`.

**`ui_utils.py`** — UI-agnostic helpers: size and duration formatting/parsing (`format_size`, `split_size_bytes`, `parse_size_bytes`, `split_duration_seconds`, `parse_duration_seconds`), S3 key composition (`compose_s3_key`), signed URL command generation (`build_signed_url_commands`), and package metadata (`load_package_info`).

## Git Conventions for AI Agents

- Prepend all commit messages with `[AI]`, e.g. `[AI] fix typo in README`.
- Before pushing, verify the commit was correctly signed: run `git log --show-signature -1` and confirm the GPG signature is valid. Do not push if the signature is missing or invalid.

## Testing Patterns

Tests use `unittest.TestCase` with hand-written fake objects (`FakeService`, `FakeS3Client`) rather than `unittest.mock`. Tests do not require AWS credentials or a running S3 service — all external I/O is replaced by fakes that implement the same interface as boto3 clients.
