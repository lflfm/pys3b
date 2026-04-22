# Plan

## Bug fix: version listing not used when expanding sub-folders

When "Show Versions" is toggled on, the top-level bucket listing correctly calls
`presenter.list_object_versions`, but three code paths in `qt_view.py` still
hard-call `presenter.list_objects` regardless of the toggle:

1. `_handle_tree_open` — expanding a prefix node (lazy load)
2. `_refresh_selected_folder` — "Refresh" action on a folder context menu
3. `_handle_tree_double_click` (load_more branch) — "Load more…" pagination

Because these paths use `list_objects`, sub-folders never carry version/delete-marker
data, so deeply-nested deleted files only produce an empty prefix node at the first
level instead of the full path.

---

**Step 1 — Route prefix expansion and pagination through `list_object_versions` when
`_show_versions` is True**

In `qt_view.py`:

- `_handle_tree_open`: when `self._show_versions` is True, call
  `presenter.list_object_versions(bucket_name=…, prefix=…, on_success=…, on_error=…)`
  instead of `presenter.list_objects`.
- `_refresh_selected_folder`: same routing for the "Refresh folder" path.
- `_handle_tree_double_click` (load_more branch): same routing for pagination; pass
  `continuation_token` from `node_info.continuation_token`.

No service, controller, or presenter changes are needed — all three paths are already
wired. Only `qt_view.py` and tests need updating.
