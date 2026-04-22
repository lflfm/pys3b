# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Environment Setup

Always use a virtual environment for development and testing. Create and activate one before installing dependencies or running any commands:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

Activate the venv at the start of every session. All commands below assume the venv is active.

## Commands

```bash
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

Commit message format: `[AI] [feature-name] [wip] short description`

- Always prepend `[AI]`.
- Add `[feature-name]` to group commits belonging to a feature, e.g. `[signed-urls]`, `[uploads]`. Use the same tag consistently across all commits for that feature.
- Add `[wip]` when the commit leaves the feature incomplete (work in progress). Omit it on the final commit that completes the feature.
- Examples:
  - `[AI] [dark-mode] [wip] add theme toggle button`
  - `[AI] [dark-mode] apply theme to all dialogs`
  - `[AI] fix typo in README` (no feature tag needed for minor/standalone changes)
- Before pushing, verify the commit was correctly signed: run `git log --show-signature -1` and confirm the GPG signature is valid. Do not push if the signature is missing or invalid.

## Feature Planning (AGENTS_PLAN.md)

When undertaking a non-trivial task, document your plan in `AGENTS_PLAN.md` before writing code. Keep it current as work progresses: mark each step done (e.g. prefix with `[done]`) as soon as it is fully implemented, and replace the file contents with `_No active plan._` once all steps are complete.

Commit after each completed step — do not batch multiple steps into a single commit.

Steps should be as small as possible but should be complete units of work which may involve changing multiple files. Each step (unit of work) should not break exising functionality (unless it the part of the general feature goal to disable existing functionality) and should not lead to errors due to missing "next steps" - but they can lead to "not implemented" messages.

For example, a button may be added before its action is actually implemented, until the functionality is there, the button should just show a "not implemented" message - although it would be preferable that the button be invisible until such functionality is there.

Tests should be revised and executed with success for each unit of work.

## Versioning

The project version is stored in `pyproject.toml` under `[project] version` and follows [Semantic Versioning](https://semver.org/) (`MAJOR.MINOR.PATCH`):

- Bump **MINOR** (`1.0.x` → `1.1.0`) when completing a new user-visible feature.
- Bump **PATCH** (`1.0.2` → `1.0.3`) when completing a bug fix or non-feature improvement.
- Bump **MAJOR** only for breaking changes or major rewrites; discuss with the user first.

Bump the version in the same commit as the final (non-WIP) step of a feature or fix — never in a WIP commit. A version bump commit must also update `CHANGELOG.md` (see below).

## Changelog

Maintain `CHANGELOG.md` in the project root using the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

**Structure:**
```
## [Unreleased]
### Added
- ...

## [1.0.2] - 2025-12
### Added
- ...
```

**Rules:**
- Keep an `[Unreleased]` section at the top at all times.
- Add an entry under `[Unreleased]` in the same commit as each completed feature step (any non-WIP commit that adds user-visible behaviour). Use the categories `Added`, `Changed`, `Fixed`, or `Removed` as appropriate.
- When bumping the version, rename `[Unreleased]` to `[X.Y.Z] - YYYY-MM` (today's date, but no day) and add a fresh empty `[Unreleased]` section above it. Do this in the same commit as the `pyproject.toml` version bump.
- Entries should be short user-facing descriptions, not technical commit messages. Write from the user's perspective: *"Bucket info dialog shows versioning status and region."* not *"Added BucketInfo dataclass and wired presenter callback."*

## Testing Patterns

Tests use `unittest.TestCase` with hand-written fake objects (`FakeService`, `FakeS3Client`) rather than `unittest.mock`. Tests do not require AWS credentials or a running S3 service — all external I/O is replaced by fakes that implement the same interface as boto3 clients.
