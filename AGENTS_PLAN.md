# Plan

## Feature: bucket-info

Add an info button to the left of the bucket name label that opens a dialog with bucket metadata (versioning status, location, etc.).

### Steps

- [ ] **services.py** — Add `get_bucket_info()` to `S3BrowserService`. Calls `get_bucket_versioning()` and `get_bucket_location()`. Returns a new `BucketInfo` dataclass.
- [ ] **models.py** — Add `BucketInfo` dataclass: `name`, `region`, `versioning_status` (`"Enabled"` | `"Suspended"` | `"Disabled"`), `mfa_delete` (`"Enabled"` | `"Disabled"` | `None`).
- [ ] **controller.py** — Add `get_bucket_info(bucket_name)` delegating to service.
- [ ] **presenter.py** — Add `get_bucket_info(bucket_name, on_success, on_error)` running in background thread.
- [ ] **qt_view.py** — Add `BucketInfoDialog` (pattern: same async load + display as `ObjectDetailsDialog`). Shows: bucket name, region, versioning status, MFA delete.
- [ ] **qt_view.py** — Add info `QPushButton` (ℹ icon) to the left of `bucket_value_label` in `_create_widgets()`. Enabled only when a bucket is selected. Clicking opens `BucketInfoDialog`.
- [ ] **tests** — Add `FakeService` coverage for `get_bucket_info` in `test_controller.py`; add `test_services.py` cases for `get_bucket_info`.

---

## Feature: versioning

Show object versions in the file tree and allow per-version operations (download, delete, restore as current).

### Steps

- [ ] **models.py** — Add `ObjectVersion` dataclass: `key`, `version_id`, `last_modified`, `is_latest` (bool), `is_delete_marker` (bool), `size` (Optional[int]), `etag` (Optional[str]), `storage_class` (Optional[str]).
- [ ] **models.py** — Add `version_id: Optional[str] = None` to `NodeInfo` in `qt_view.py` (it lives there, not models.py).
- [ ] **services.py** — Add `list_object_versions(*, endpoint_url, access_key, secret_key, bucket_name, prefix, key_marker, version_id_marker, max_keys)` using `client.list_object_versions()`. Returns `list[ObjectVersion]`.
- [ ] **services.py** — Add `delete_object_version(*, ..., bucket_name, key, version_id)` using `client.delete_object(Bucket=..., Key=..., VersionId=...)`.
- [ ] **services.py** — Add `download_object_version(*, ..., bucket_name, key, version_id, local_path, progress_callback, cancel_callback)` using `client.download_file()` with `ExtraArgs={'VersionId': version_id}`.
- [ ] **services.py** — Add `restore_object_version(*, ..., bucket_name, key, version_id)` — copies the version to itself without a VersionId (making it the new current) using `client.copy_object()`.
- [ ] **controller.py** — Add `list_object_versions`, `delete_object_version`, `download_object_version`, `restore_object_version` delegating to service.
- [ ] **presenter.py** — Add corresponding async presenter methods with `on_success`/`on_error` callbacks.
- [ ] **qt_view.py** — Add `version_id: Optional[str] = None` field to `NodeInfo`; add `node_type = "version"` and `node_type = "delete_marker"` handling throughout.
- [ ] **qt_view.py** — Add a "Show versions" toggle button to the toolbar (only enabled when bucket is selected and versioning is enabled — detected via `BucketInfo`). State stored as `self._show_versions: bool`.
- [ ] **qt_view.py** — When `_show_versions` is True and an object node is expanded, lazy-load its versions via `presenter.list_object_versions()` and insert them as children using a new `_insert_version_node()` helper.
- [ ] **qt_view.py** — Version rows display: version ID (truncated), last modified, size, "latest" badge or "delete marker" label.
- [ ] **qt_view.py** — Context menu for version nodes: "Download this version", "Delete this version", "Restore as current" (hidden for delete markers where not applicable).
- [ ] **tests** — Add fake/service/controller/presenter coverage for all new version methods.
