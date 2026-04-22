# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2026-04

### Added
- "Show Versions" toggle in the Objects menu: when enabled, fetches all object versions and displays them as child nodes in the tree (delete markers shown with `[deleted]` prefix, latest version marked with ★).
- Version node context menu: Info, Download this version, Delete this version.
- Version-specific download: downloads the selected version directly.
- Version-specific delete: permanently removes a single version after confirmation.
- Object details dialog shows Version ID when viewing a specific version.

## [1.1.0] - 2026-04

### Added
- Bucket info button (ⓘ) next to the bucket name shows versioning status and region in a dialog.

## [1.0.2] - 2026-01

### Changed
- Signed URL dialog: choose duration unit (seconds, minutes, hours, days) and save personal default in settings.

## [1.0.1] - 2026-01

### Added
- Drag-and-drop uploads: configurable destination directory per drop.

## [1.0.0] - 2026-01

### Added
- Multi-file download and delete support.
- Drag-and-drop file upload.
- Multipart upload with configurable threshold, chunk size, and concurrency settings.
- Signed URL generation (GET and POST) with clipboard copy commands.
- Object details dialog showing size, last modified, storage class, ETag, content type, checksums, and custom metadata.
- Persistent connection profiles with OS keychain integration for secret keys.
- Remembers the last used connection across sessions.
- Full UI rewrite using PySide6.
