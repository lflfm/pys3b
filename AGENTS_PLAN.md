# Plan

## Features: Bucket Info Button + S3 Versioning

---

### Phase 1 — Bucket Info Button

**Step 1 — Data model: `BucketInfo`**
Add `BucketInfo` dataclass to `models.py` with fields:
- `name: str`
- `versioning_status: str` — one of `"Enabled"`, `"Suspended"`, `"Disabled"`, `"Unknown"`
- `region: Optional[str]`

**Step 2 — Service: `get_bucket_info()`**
Add `S3BrowserService.get_bucket_info(bucket: str) -> BucketInfo` to `services.py`.
Calls `client.get_bucket_versioning(Bucket=bucket)` and `client.get_bucket_location(Bucket=bucket)`.
Maps the versioning `Status` field (`"Enabled"` / `"Suspended"` / missing → `"Disabled"`).

**Step 3 — Controller + Presenter: `get_bucket_info()`**
- `controller.py`: add `get_bucket_info(bucket)` that calls `_require_connection()` then delegates to service.
- `presenter.py`: add `get_bucket_info(bucket, on_result, on_error)` following the existing threading/callback pattern.

**Step 4 — UI: info button + `BucketInfoDialog`**
- In `qt_view.py` `_create_widgets()`: insert a small `QToolButton` with text `"ⓘ"` (or icon) between the `"Bucket:"` label and `bucket_value_label`. Disable it when no bucket is selected; enable/disable it alongside upload/download controls.
- Add `BucketInfoDialog(QDialog)` that displays: Bucket name, Region, Versioning status.
- Wire button click → call `presenter.get_bucket_info()` → open dialog on result.
- Add tests for `get_bucket_info` in `FakeService`/`FakeS3Client`.

---

### Phase 2 — Versioning Foundation

**Step 5 — Data models: `ObjectVersion`**
Add `ObjectVersion` dataclass to `models.py`:
- `version_id: str`
- `last_modified: Optional[datetime]`
- `size: Optional[int]`
- `etag: Optional[str]`
- `storage_class: Optional[str]`
- `is_latest: bool`
- `is_delete_marker: bool`

Add `versions: list[ObjectVersion]` to `ObjectPage` (default empty list).
Add optional `version_id: Optional[str]` field to `ObjectDetails`.

**Step 6 — Service: `list_object_versions()`**
Add `S3BrowserService.list_object_versions(bucket, prefix, delimiter, continuation_token) -> BucketListing` to `services.py`.
Uses boto3 `list_object_versions` API. Returns a `BucketListing` whose `ObjectPage` entries carry `versions` (both `Versions` and `DeleteMarkers` from the API response). Update `get_object_details()` to accept and pass an optional `version_id` to `head_object`.

**Step 7 — Controller + Presenter: version listing + details**
- `controller.py`: add `list_object_versions(bucket, prefix, delimiter, continuation_token)` delegating to service. Update `get_object_details()` signature to accept `version_id=None`.
- `presenter.py`: add `list_object_versions(...)` following the existing callback pattern. Update `get_object_details()` to pass `version_id`.

---

### Phase 3 — Versioning UI

**Step 8 — Tree: version nodes + "Show Versions" toggle**
- In `qt_view.py`: extend `NodeInfo` with a `version_id: Optional[str]` field; add `node_type = "version"`.
- Add `"Show Versions"` checkable action to the **Objects** menu. When checked, expand file nodes to load and display version child nodes (each showing version ID suffix, last-modified, size, delete-marker indicator). When unchecked, collapse/hide version nodes.
- Update `_populate_tree` / `_render_listing_contents` / `_insert_file_node` / `_handle_tree_open` to handle version child nodes when the toggle is on.
- Version nodes for delete markers display with a distinct label (e.g. `[deleted]`).

**Step 9 — Download: version-aware**
- `services.py` `download_object()`: accept optional `version_id` kwarg; pass to boto3 `download_fileobj` via `ExtraArgs`.
- `controller.py`, `presenter.py`: thread `version_id` through.
- `qt_view.py`: when a version node is selected and Download is triggered, pass `version_id`. Add "Download this version" to the version-node context menu.

**Step 10 — Delete: version-aware**
- `services.py` `delete_object()`: accept optional `version_id`; pass to boto3 `delete_object`.
- `controller.py`, `presenter.py`: thread `version_id` through.
- `qt_view.py`: add "Delete this version" to the version-node context menu. Confirm before deleting.

**Step 11 — Object details: show version info**
- Update `ObjectDetailsDialog` to display `Version ID` and `Is Latest` fields when `version_id` is present on the `ObjectDetails` result.
- Wiring: double-clicking a version node calls `get_object_details` with the version's `version_id`.
