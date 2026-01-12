from __future__ import annotations
"""PySide6-based UI for the S3 browser application."""
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Callable

from PySide6 import QtCore, QtGui, QtWidgets

from .models import BucketListing, ObjectDetails
from .presenter import S3BrowserPresenter
from .profiles import ConnectionProfile
from .settings import AppSettings
from .ui_utils import (
    PackageInfo,
    build_signed_url_commands,
    compose_s3_key,
    format_last_modified,
    format_size,
    parse_size_bytes,
    split_size_bytes,
    suggest_command_filename,
)

NODE_ID_ROLE = QtCore.Qt.UserRole + 1
LOGGER = logging.getLogger(__name__)


class _DispatchBridge(QtCore.QObject):
    run = QtCore.Signal(object)


@dataclass
class NodeInfo:
    node_type: str
    bucket: str
    prefix: str | None = None
    key: str | None = None
    delimiter: str | None = None
    continuation_token: str | None = None
    loaded: bool = False
    loading: bool = False
    parent_id: str | None = None


class UploadDropTreeView(QtWidgets.QTreeView):
    """Tree view that accepts file drops for uploading."""

    def __init__(
        self,
        parent: QtWidgets.QWidget | None = None,
        *,
        drop_allowed: Callable[[], bool],
        handle_drop: Callable[[list[QtCore.QUrl], QtCore.QModelIndex], None],
    ) -> None:
        super().__init__(parent)
        self._drop_allowed = drop_allowed
        self._handle_drop = handle_drop
        self.setAcceptDrops(True)
        self.setDropIndicatorShown(True)
        self.setDragDropMode(QtWidgets.QAbstractItemView.DropOnly)

    def dragEnterEvent(self, event: QtGui.QDragEnterEvent) -> None:
        if self._can_accept(event):
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event: QtGui.QDragMoveEvent) -> None:
        if self._can_accept(event):
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event: QtGui.QDropEvent) -> None:
        if not self._can_accept(event):
            event.ignore()
            return
        pos = event.position().toPoint() if hasattr(event, "position") else event.pos()
        target_index = self.indexAt(pos)
        urls = list(event.mimeData().urls() or [])
        self._handle_drop(urls, target_index)
        event.acceptProposedAction()

    def _can_accept(self, event: QtGui.QDropEvent) -> bool:
        if not self._drop_allowed():
            return False
        mime = event.mimeData()
        return bool(mime and mime.hasUrls())


class S3BrowserWindow(QtWidgets.QMainWindow):
    """Main window for the S3 browser UI."""

    def __init__(self, presenter: S3BrowserPresenter | None = None):
        super().__init__()
        self.setWindowTitle("S3 Object Browser")
        self.resize(900, 900)
        self.setMinimumSize(640, 480)

        self._dispatch_bridge = _DispatchBridge()
        self._dispatch_bridge.run.connect(lambda func: func())
        self.presenter = presenter or S3BrowserPresenter(dispatch=self._dispatch)
        self._settings = self.presenter.settings
        self._package_info = self.presenter.package_info
        self._current_max_keys = self._settings.fetch_limit if self._settings.fetch_limit > 0 else 10
        self._operation_in_progress = False
        self._pending_object_refresh = False
        self._bucket_names: list[str] = []
        self._node_state: dict[str, NodeInfo] = {}
        self._node_items: dict[str, QtGui.QStandardItem] = {}
        self._transfer_dialog: TransferDialog | None = None

        self._selected_connection: str = ""
        self._selected_bucket: str = ""

        self._create_menu()
        self._create_widgets()
        self._create_context_menus()
        self._refresh_connection_menu()
        self._render_bucket_menu()
        self._auto_connect_if_enabled()

    def _dispatch(self, func: Callable[[], None]) -> None:
        self._dispatch_bridge.run.emit(func)

    def _create_menu(self) -> None:
        menubar = self.menuBar()

        self.file_menu = menubar.addMenu("File")
        self.upload_action = self.file_menu.addAction("Upload...")
        self.upload_action.triggered.connect(self.upload_file)
        self.upload_action.setEnabled(False)

        self.download_action = self.file_menu.addAction("Download File...")
        self.download_action.triggered.connect(self._download_selected_objects)
        self.download_action.setEnabled(False)

        self.signed_url_action = self.file_menu.addAction("Generate Signed URL...")
        self.signed_url_action.triggered.connect(self.open_signed_url_dialog)
        self.signed_url_action.setEnabled(False)

        self.file_menu.addSeparator()
        exit_action = self.file_menu.addAction("Exit")
        exit_action.triggered.connect(self.close)

        self.connection_menu = menubar.addMenu("Connection")
        self.bucket_menu = menubar.addMenu("Buckets")

        self.objects_menu = menubar.addMenu("Objects")
        self.objects_refresh_action = self.objects_menu.addAction("Refresh")
        self.objects_refresh_action.triggered.connect(self.list_objects)
        self.objects_refresh_action.setEnabled(False)

        options_menu = menubar.addMenu("Options")
        settings_action = options_menu.addAction("Settings")
        settings_action.triggered.connect(self.open_settings_dialog)

        help_menu = menubar.addMenu("Help")
        about_action = help_menu.addAction("About")
        about_action.triggered.connect(self.show_about_dialog)

    def _create_widgets(self) -> None:
        central = QtWidgets.QWidget(self)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        bucket_row = QtWidgets.QHBoxLayout()
        bucket_label = QtWidgets.QLabel("Bucket:")
        self.bucket_value_label = QtWidgets.QLabel("No bucket selected")
        self.bucket_value_label.setMinimumWidth(240)
        bucket_row.addWidget(bucket_label)
        bucket_row.addWidget(self.bucket_value_label, stretch=1)

        self.upload_button = QtWidgets.QPushButton("Upload File")
        self.upload_button.clicked.connect(self.upload_file)
        self.upload_button.setEnabled(False)
        bucket_row.addWidget(self.upload_button)
        self.download_button = QtWidgets.QPushButton("Download")
        self.download_button.clicked.connect(self._download_selected_objects)
        self.download_button.setEnabled(False)
        bucket_row.addWidget(self.download_button)
        layout.addLayout(bucket_row)

        self.results_tree = UploadDropTreeView(
            self,
            drop_allowed=self._allow_tree_drop,
            handle_drop=self._handle_tree_drop,
        )
        self.results_tree.setHeaderHidden(True)
        self.results_tree.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.results_tree.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.results_tree.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.results_tree.customContextMenuRequested.connect(self._handle_tree_right_click)
        self.results_tree.expanded.connect(self._handle_tree_open)
        self.results_tree.doubleClicked.connect(self._handle_tree_double_click)

        self._model = QtGui.QStandardItemModel(0, 1, self)
        self.results_tree.setModel(self._model)
        self.results_tree.selectionModel().selectionChanged.connect(self._refresh_selection_controls)
        layout.addWidget(self.results_tree, stretch=1)

        self.progress = QtWidgets.QProgressBar(self)
        self.progress.setRange(0, 1)
        self.progress.setValue(0)
        self.progress.setVisible(False)
        layout.addWidget(self.progress)

        self.status_label = QtWidgets.QLabel("Ready")
        layout.addWidget(self.status_label)

        self.setCentralWidget(central)

    def _create_context_menus(self) -> None:
        self.object_menu = QtWidgets.QMenu(self)
        self.object_menu.addAction("Info", self._open_selected_object_info)
        self.object_menu.addAction("Download", self._download_selected_objects)
        self.object_menu.addAction("Get Signed URL", self._open_signed_url_for_selection)
        self.object_menu.addSeparator()
        self.object_menu.addAction("Delete", self._delete_selected_objects)

        self.object_multi_menu = QtWidgets.QMenu(self)
        self.object_multi_menu.addAction("Download", self._download_selected_objects)
        self.object_multi_menu.addAction("Delete", self._delete_selected_objects)

        self.folder_menu = QtWidgets.QMenu(self)
        self.folder_menu.addAction("Upload...", self.upload_file)
        self.folder_menu.addAction("Refresh", self._refresh_selected_folder)
        self.folder_menu.addAction("Get Signed URL", self._open_signed_url_for_selection)

    def show_about_dialog(self, *_: object) -> None:
        dialog = AboutDialog(self, package_info=self._package_info)
        dialog.exec()

    def open_settings_dialog(self, *_: object) -> None:
        dialog = SettingsDialog(self, settings=self._settings)
        if dialog.exec() != QtWidgets.QDialog.Accepted:
            return
        new_settings = dialog.result_settings
        if not new_settings:
            return
        self._settings = new_settings
        self.presenter.save_settings(self._settings)
        self._current_max_keys = max(self._settings.fetch_limit, 1)
        self._refresh_connection_menu()
        self._render_bucket_menu()
        if self._selected_bucket:
            self._schedule_object_refresh()

    def create_connection(self, *, connect_on_save: bool = False, **_: object) -> None:
        primary_action = "save_and_connect" if connect_on_save else "save"
        primary_label = "Save and Connect" if connect_on_save else "Save"
        dialog = ConnectionDialog(
            self,
            title="Create Connection",
            primary_action=primary_action,
            primary_label=primary_label,
        )
        self._apply_connection_dialog_result(dialog.exec_and_get())

    def edit_connection(
        self,
        profile_name: str | None = None,
        *,
        connect_on_save: bool = False,
        **_: object,
    ) -> None:
        target_name = profile_name or self._selected_connection
        if not target_name:
            return
        try:
            profile = self.presenter.get_profile(target_name)
        except ValueError as exc:
            self._show_error("Error", str(exc))
            return
        primary_action = "save_and_connect" if connect_on_save else "save"
        primary_label = "Save and Connect" if connect_on_save else "Save"
        dialog = ConnectionDialog(
            self,
            title="Edit Connection",
            profile=profile,
            primary_action=primary_action,
            primary_label=primary_label,
        )
        result = dialog.exec_and_get()
        self._apply_connection_dialog_result(result)

    def _apply_connection_dialog_result(self, result: dict | None) -> None:
        if not result:
            LOGGER.debug("Connection dialog dismissed without action")
            return
        action = result["action"]
        LOGGER.debug("Connection dialog action: %s", action)
        if action == "connect":
            self.connect(result["name"])
        elif action in {"save", "save_and_connect"}:
            profile: ConnectionProfile = result["profile"]
            try:
                self.presenter.save_profile(profile, original_name=result.get("original_name"))
            except ValueError as exc:
                self._show_error("Error", str(exc))
                return
            self._refresh_connection_menu(selected_name=profile.name)
            if action == "save_and_connect":
                self.connect(profile.name)
        elif action == "delete":
            try:
                self.presenter.delete_profile(result["name"])
            except ValueError as exc:
                self._show_error("Error", str(exc))
                return
            if self._settings.remember_last_bucket and result["name"] == self._settings.last_connection:
                self._settings = AppSettings(
                    fetch_limit=self._settings.fetch_limit,
                    default_post_max_size=self._settings.default_post_max_size,
                    upload_multipart_threshold=self._settings.upload_multipart_threshold,
                    upload_chunk_size=self._settings.upload_chunk_size,
                    upload_max_concurrency=self._settings.upload_max_concurrency,
                    remember_last_bucket=self._settings.remember_last_bucket,
                    last_bucket=self._settings.last_bucket,
                    last_connection="",
                )
                self.presenter.save_settings(self._settings)
            self._refresh_connection_menu()

    def _refresh_connection_menu(self, selected_name: str | None = None) -> None:
        profiles = self.presenter.list_profiles()
        names = [profile.name for profile in profiles]
        current = self._selected_connection
        if selected_name and selected_name in names:
            self._selected_connection = selected_name
        elif current in names:
            pass
        elif names:
            preferred = ""
            if self._settings.remember_last_bucket:
                candidate = self._settings.last_connection
                if candidate in names:
                    preferred = candidate
            self._selected_connection = preferred or names[0]
        else:
            self._selected_connection = ""

        self.connection_menu.clear()
        create_action = self.connection_menu.addAction("Create New Connection")
        create_action.triggered.connect(self.create_connection)
        self.connection_menu.addSeparator()
        if names:
            for name in names:
                action = self.connection_menu.addAction(name)
                action.triggered.connect(lambda _, value=name: self._open_connection_from_menu(value))
        else:
            action = self.connection_menu.addAction("No saved connections")
            action.setEnabled(False)

        self._refresh_upload_controls()
        self._refresh_signed_url_controls()

    def _open_connection_from_menu(self, profile_name: str) -> None:
        self.edit_connection(profile_name=profile_name, connect_on_save=True)

    def connect(self, profile_name: str | None = None) -> None:
        target_name = profile_name or self._selected_connection
        if not target_name:
            self._show_error("Error", "Please choose a connection from the Connection menu")
            return
        self._selected_connection = target_name

        self._start_operation()
        LOGGER.debug("Starting connect flow for profile '%s'", target_name)

        def handle_success(buckets: list[str]) -> None:
            self.presenter.update_last_connection(target_name)
            self._update_bucket_menu(buckets)
            self._refresh_connection_menu()
            self._set_status("Connected. Buckets loaded.")

        self.presenter.connect(
            profile_name=target_name,
            on_success=handle_success,
            on_error=lambda msg: self._show_error("Connection Error", f"Error connecting to S3: {msg}"),
            on_done=self._end_operation,
        )

    def refresh_buckets(self, *_: object) -> None:
        if not self.presenter.is_connected:
            self._show_error("Error", "Please connect first")
            return

        self._start_operation()
        self.presenter.refresh_buckets(
            on_success=self._update_bucket_menu,
            on_error=lambda msg: self._show_error("Bucket Error", f"Error refreshing buckets: {msg}"),
            on_done=self._end_operation,
        )

    def list_objects(self, *_: object) -> None:
        self._pending_object_refresh = False
        if not self.presenter.is_connected:
            self._show_error("Error", "Please connect first")
            return

        bucket_name = self._selected_bucket
        if not bucket_name:
            self._show_error("Error", "Please select a bucket")
            return

        max_keys = max(self._current_max_keys, 1)
        self._current_max_keys = max_keys
        self._clear_tree()
        self._start_operation()

        def handle_success(listing: BucketListing) -> None:
            self._populate_tree([listing])

        self.presenter.list_objects(
            bucket_name=bucket_name,
            max_keys=max_keys,
            on_success=handle_success,
            on_error=lambda msg: self._show_error("List Error", f"Error listing objects: {msg}"),
            on_done=self._end_operation,
        )

    def _auto_connect_if_enabled(self) -> None:
        last_connection = self.presenter.maybe_auto_connect_profile()
        if not last_connection:
            return
        names = [profile.name for profile in self.presenter.list_profiles()]
        if last_connection not in names:
            return
        self._selected_connection = last_connection
        self._dispatch(lambda: self.connect(last_connection))

    def _update_bucket_menu(self, buckets: list[str]) -> None:
        LOGGER.debug("Updating bucket menu with %d bucket(s)", len(buckets))
        self._bucket_names = list(buckets)
        current = self._selected_bucket
        if current not in self._bucket_names:
            preferred = ""
            if self._settings.remember_last_bucket:
                candidate = self._settings.last_bucket
                if candidate in self._bucket_names:
                    preferred = candidate
            new_value = preferred or (self._bucket_names[0] if self._bucket_names else "")
            if new_value != current:
                self._selected_bucket = new_value
                if new_value:
                    self._on_bucket_selected()
            else:
                self._selected_bucket = new_value
        else:
            if current:
                self._schedule_object_refresh()
        self._render_bucket_menu()
        self._refresh_upload_controls()

    def _render_bucket_menu(self) -> None:
        self.bucket_menu.clear()
        refresh_action = self.bucket_menu.addAction("Refresh Buckets")
        refresh_action.triggered.connect(self.refresh_buckets)
        refresh_action.setEnabled(self.presenter.is_connected and not self._operation_in_progress)
        self.bucket_menu.addSeparator()
        if self._bucket_names:
            for bucket in self._bucket_names:
                action = self.bucket_menu.addAction(bucket)
                action.triggered.connect(lambda _, value=bucket: self._select_bucket_from_menu(value))
        else:
            action = self.bucket_menu.addAction("No buckets")
            action.setEnabled(False)

    def _select_bucket_from_menu(self, bucket_name: str) -> None:
        if not bucket_name:
            return
        self._selected_bucket = bucket_name
        self._on_bucket_selected()

    def _start_operation(self) -> None:
        self._operation_in_progress = True
        self.progress.setRange(0, 0)
        self.progress.setVisible(True)
        self._refresh_upload_controls()
        self._refresh_signed_url_controls()
        self._set_objects_menu_state(False)
        self._set_download_controls_state(False)
        self._render_bucket_menu()

    def _end_operation(self) -> None:
        self._operation_in_progress = False
        self.progress.setVisible(False)
        self.progress.setRange(0, 1)
        self._refresh_upload_controls()
        self._refresh_signed_url_controls()
        self._refresh_selection_controls()
        self._render_bucket_menu()

    def _on_bucket_selected(self) -> None:
        self.bucket_value_label.setText(self._selected_bucket or "No bucket selected")
        self.presenter.update_last_bucket(self._selected_bucket)
        self._schedule_object_refresh()

    def _schedule_object_refresh(self) -> None:
        if self._pending_object_refresh:
            return
        self._pending_object_refresh = True
        QtCore.QTimer.singleShot(150, self._perform_pending_object_refresh)

    def _perform_pending_object_refresh(self) -> None:
        if not self._pending_object_refresh:
            return
        self._pending_object_refresh = False
        self.list_objects()

    def _set_objects_menu_state(self, enabled: bool) -> None:
        self.objects_refresh_action.setEnabled(enabled)

    def _refresh_upload_controls(self) -> None:
        enabled = bool(self._selected_connection and self._selected_bucket and not self._operation_in_progress)
        self.upload_action.setEnabled(enabled)
        self.upload_button.setEnabled(enabled)

    def _set_download_controls_state(self, enabled: bool) -> None:
        self.download_action.setEnabled(enabled)
        self.download_button.setEnabled(enabled)

    def _refresh_signed_url_controls(self) -> None:
        enabled = bool(self._selected_connection and self._selected_bucket and not self._operation_in_progress)
        self.signed_url_action.setEnabled(enabled)

    def _refresh_selection_controls(self, *_: object) -> None:
        selected_objects = self._get_selected_objects()
        if not selected_objects:
            self._set_download_controls_state(False)
            self._set_objects_menu_state(self.presenter.is_connected and not self._operation_in_progress)
            return
        self._set_objects_menu_state(self.presenter.is_connected and not self._operation_in_progress)
        self._set_download_controls_state(not self._operation_in_progress)

    def _refresh_selected_folder(self, *_: object) -> None:
        selected = self._get_selected_node()
        if not selected:
            return
        node_id, node_info = selected
        if node_info.node_type not in {"prefix", "bucket"}:
            return
        if node_info.node_type == "bucket":
            self.list_objects()
            return
        self._start_operation()

        def handle_success(listing: BucketListing) -> None:
            self._render_prefix_listing(node_id, listing)

        self.presenter.list_objects(
            bucket_name=node_info.bucket,
            max_keys=self._current_max_keys,
            prefix=node_info.prefix or "",
            on_success=handle_success,
            on_error=lambda msg: self._handle_prefix_error(node_id, msg),
            on_done=self._end_operation,
        )

    def _get_selected_node(self) -> tuple[str, NodeInfo] | None:
        index = self.results_tree.currentIndex()
        if not index.isValid():
            return None
        item = self._model.itemFromIndex(index)
        if not item:
            return None
        node_id = item.data(NODE_ID_ROLE)
        if not node_id:
            return None
        node_info = self._node_state.get(node_id)
        if not node_info:
            return None
        return node_id, node_info

    def _get_selected_objects(self) -> list[tuple[str, str]]:
        selection_model = self.results_tree.selectionModel()
        if not selection_model:
            return []
        selected_indexes = selection_model.selectedRows(0)
        seen: set[str] = set()
        objects: list[tuple[str, str]] = []
        for index in selected_indexes:
            item = self._model.itemFromIndex(index)
            if not item:
                continue
            node_id = item.data(NODE_ID_ROLE)
            if not node_id or node_id in seen:
                continue
            seen.add(node_id)
            info = self._node_state.get(node_id)
            if not info or info.node_type != "object":
                continue
            objects.append((info.bucket, info.key or ""))
        return objects

    def _get_selected_upload_target(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            if self._selected_bucket:
                return self._selected_bucket, ""
            return None
        _, info = selected
        if info.node_type == "object":
            return info.bucket, os.path.dirname(info.key or "") + "/" if info.key else ""
        if info.node_type in {"prefix", "bucket"}:
            return info.bucket, info.prefix or ""
        return None

    def _get_upload_target_from_index(self, index: QtCore.QModelIndex) -> tuple[str, str] | None:
        if index.isValid():
            item = self._model.itemFromIndex(index)
            if item:
                node_id = item.data(NODE_ID_ROLE)
                if node_id:
                    node_info = self._node_state.get(node_id)
                    if node_info:
                        return self._get_upload_target_for_node(node_info)
        if self._selected_bucket:
            return self._selected_bucket, ""
        return None

    def _get_upload_target_for_node(self, info: NodeInfo) -> tuple[str, str] | None:
        if info.node_type == "object":
            if not info.key:
                return info.bucket, ""
            prefix = os.path.dirname(info.key)
            return info.bucket, f"{prefix}/" if prefix else ""
        if info.node_type == "prefix":
            return info.bucket, info.prefix or ""
        if info.node_type == "bucket":
            return info.bucket, ""
        return None

    def _allow_tree_drop(self) -> bool:
        return bool(
            self.presenter.is_connected
            and not self._operation_in_progress
        )

    def _handle_tree_drop(self, urls: list[QtCore.QUrl], index: QtCore.QModelIndex) -> None:
        selection = self._get_upload_target_from_index(index)
        if not selection:
            self._show_error("Upload Error", "Please select a bucket or folder")
            return
        bucket, prefix = selection
        file_paths = []
        for url in urls:
            if not url.isLocalFile():
                continue
            path = url.toLocalFile()
            if os.path.isfile(path):
                file_paths.append(path)
        if not file_paths:
            self._show_error("Upload Error", "Only local files can be uploaded.")
            return
        if len(file_paths) == 1:
            self._upload_path_with_dialog(bucket, prefix, file_paths[0])
            return
        dialog = UploadMultipleDialog(
            self,
            bucket=bucket,
            file_count=len(file_paths),
            initial_prefix=prefix,
        )
        result = dialog.exec_and_get()
        if not result:
            return
        self._upload_files_sequential(bucket, result["prefix"], file_paths)

    def _get_selected_object_path(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            return None
        _, info = selected
        if info.node_type == "object":
            return info.bucket, info.key or ""
        if info.node_type == "prefix":
            return info.bucket, info.prefix or ""
        if info.node_type == "bucket":
            return info.bucket, ""
        return None

    def _get_selected_object(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            return None
        _, info = selected
        if info.node_type != "object":
            return None
        return info.bucket, info.key or ""

    def _open_selected_object_info(self, *_: object) -> None:
        selected = self._get_selected_object()
        if not selected:
            return
        bucket, key = selected
        self._show_object_details(bucket, key)

    def _download_selected_objects(self, *_: object) -> None:
        selected_objects = self._get_selected_objects()
        if not selected_objects:
            return
        if len(selected_objects) == 1:
            bucket, key = selected_objects[0]
            self._download_object(bucket, key)
            return
        target_dir = QtWidgets.QFileDialog.getExistingDirectory(self, "Select download folder")
        if not target_dir:
            return
        self._download_objects_sequential(selected_objects, target_dir)

    def _delete_selected_objects(self, *_: object) -> None:
        selected_objects = self._get_selected_objects()
        if not selected_objects:
            return
        if len(selected_objects) == 1:
            bucket, key = selected_objects[0]
            self._delete_object(bucket, key)
            return
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Delete Objects",
            f"Delete {len(selected_objects)} objects?",
        )
        if confirm != QtWidgets.QMessageBox.Yes:
            return
        self._delete_objects_sequential(selected_objects)

    def _open_signed_url_for_selection(self, *_: object) -> None:
        selection = self._get_selected_object_path()
        if not selection:
            return
        bucket, key = selection
        self.open_signed_url_dialog(bucket=bucket, key=key)

    def _populate_tree(self, bucket_listings: list[BucketListing]) -> None:
        self._clear_tree()
        total_objects = 0
        total_prefixes = 0

        root = self._model.invisibleRootItem()
        for bucket in bucket_listings:
            bucket_item = QtGui.QStandardItem(bucket.name)
            bucket_item.setEditable(False)
            bucket_id = f"bucket:{bucket.name}"
            self._register_node(bucket_id, bucket_item, NodeInfo(node_type="bucket", bucket=bucket.name, prefix=bucket.prefix or ""))
            root.appendRow(bucket_item)

            if bucket.error:
                bucket_item.appendRow(QtGui.QStandardItem(f"Error: {bucket.error}"))
                continue

            objects_added, prefixes_added = self._render_listing_contents(bucket_item, bucket)
            total_objects += objects_added
            total_prefixes += prefixes_added
            if not (objects_added or prefixes_added):
                bucket_item.appendRow(QtGui.QStandardItem("(No objects)"))

        if total_objects or total_prefixes:
            self._set_status(f"Loaded {total_objects} object(s) and {total_prefixes} folder(s).")
        else:
            self._set_status("No objects found.")
        self._refresh_selection_controls()
        self.results_tree.expandAll()

    def _render_listing_contents(self, parent_item: QtGui.QStandardItem, listing: BucketListing) -> tuple[int, int]:
        objects_added = 0
        prefixes_added = 0
        for page in listing.pages:
            if page.error:
                parent_item.appendRow(QtGui.QStandardItem(f"Page {page.number} error: {page.error}"))
                continue
            for prefix in page.prefixes:
                self._insert_prefix_node(parent_item, listing.name, prefix, listing.prefix)
                prefixes_added += 1
            for key in page.keys:
                self._insert_file_node(parent_item, listing.name, key, listing.prefix)
                objects_added += 1
        self._refresh_load_more_node(parent_item, listing)
        return objects_added, prefixes_added

    def _insert_prefix_node(self, parent_item: QtGui.QStandardItem, bucket: str, prefix: str, base_prefix: str) -> str:
        label = self._relative_name(prefix, base_prefix)
        node_id = f"prefix:{bucket}:{prefix}"
        prefix_item = QtGui.QStandardItem(label)
        prefix_item.setEditable(False)
        prefix_item.appendRow(QtGui.QStandardItem("Loading..."))
        self._register_node(
            node_id,
            prefix_item,
            NodeInfo(node_type="prefix", bucket=bucket, prefix=prefix, loaded=False, loading=False),
        )
        parent_item.appendRow(prefix_item)
        return node_id

    def _insert_file_node(self, parent_item: QtGui.QStandardItem, bucket: str, key: str, base_prefix: str) -> None:
        label = self._relative_name(key, base_prefix)
        node_id = f"object:{bucket}:{key}"
        item = QtGui.QStandardItem(label)
        item.setEditable(False)
        self._register_node(node_id, item, NodeInfo(node_type="object", bucket=bucket, key=key))
        parent_item.appendRow(item)

    def _register_node(self, node_id: str, item: QtGui.QStandardItem, info: NodeInfo) -> None:
        item.setData(node_id, NODE_ID_ROLE)
        self._node_state[node_id] = info
        self._node_items[node_id] = item

    def _find_node(self, *, node_type: str, bucket: str, key: str | None = None, prefix: str | None = None) -> str | None:
        for node_id, info in self._node_state.items():
            if info.node_type != node_type or info.bucket != bucket:
                continue
            if key is not None and info.key != key:
                continue
            if prefix is not None and info.prefix != prefix:
                continue
            if node_id in self._node_items:
                return node_id
        return None

    def _node_has_content(self, node_item: QtGui.QStandardItem) -> bool:
        for row in range(node_item.rowCount()):
            child = node_item.child(row)
            if child and child.data(NODE_ID_ROLE):
                return True
        return False

    def _remove_placeholder_children(self, parent_item: QtGui.QStandardItem) -> None:
        placeholders = {"(No objects)", "(Empty)"}
        rows = list(range(parent_item.rowCount()))
        for row in reversed(rows):
            child = parent_item.child(row)
            if not child:
                continue
            if child.data(NODE_ID_ROLE):
                continue
            if child.text() in placeholders:
                parent_item.removeRow(row)

    def _prune_empty_parents(self, node_item: QtGui.QStandardItem) -> None:
        current = node_item
        while current:
            node_id = current.data(NODE_ID_ROLE)
            if not node_id:
                return
            node_info = self._node_state.get(node_id)
            if not node_info:
                return
            if node_info.node_type == "bucket":
                if self._node_has_content(current):
                    return
                self._remove_placeholder_children(current)
                current.appendRow(QtGui.QStandardItem("(No objects)"))
                return
            if node_info.node_type != "prefix":
                return
            if self._node_has_content(current):
                return
            parent = current.parent()
            self._delete_subtree(node_id)
            current = parent

    def _remove_object_from_tree(self, bucket: str, key: str) -> bool:
        node_id = self._find_node(node_type="object", bucket=bucket, key=key)
        if not node_id:
            return False
        item = self._node_items.get(node_id)
        if not item:
            return False
        parent = item.parent()
        self._delete_subtree(node_id)
        if parent:
            self._prune_empty_parents(parent)
        self._refresh_selection_controls()
        return True

    def _ensure_prefix_chain(self, bucket_item: QtGui.QStandardItem, bucket: str, prefix: str) -> tuple[str | None, bool]:
        segments = [segment for segment in prefix.strip("/").split("/") if segment]
        current_parent = bucket_item
        current_prefix = ""
        created = False
        for segment in segments:
            current_prefix = f"{current_prefix}{segment}/"
            existing = self._find_node(node_type="prefix", bucket=bucket, prefix=current_prefix)
            if existing:
                current_parent = self._node_items[existing]
                continue
            parent_info = self._node_state.get(current_parent.data(NODE_ID_ROLE), NodeInfo("", bucket))
            if parent_info.node_type == "prefix" and not parent_info.loaded:
                return None, True
            base_prefix = parent_info.prefix or ""
            node_id = self._insert_prefix_node(current_parent, bucket, current_prefix, base_prefix)
            current_parent = self._node_items[node_id]
            created = True
        return current_parent.data(NODE_ID_ROLE), created

    def _add_object_to_tree(self, bucket: str, key: str) -> bool:
        if bucket != self._selected_bucket:
            return False
        bucket_id = self._find_node(node_type="bucket", bucket=bucket)
        if not bucket_id:
            return False
        if self._find_node(node_type="object", bucket=bucket, key=key):
            return True
        prefix = ""
        if "/" in key:
            prefix = f"{key.rsplit('/', 1)[0]}/"
        parent_id = bucket_id
        bucket_item = self._node_items.get(bucket_id)
        if not bucket_item:
            return False
        parent_item = bucket_item
        if prefix:
            prefix_id, created = self._ensure_prefix_chain(bucket_item, bucket, prefix)
            if not prefix_id:
                return False
            prefix_info = self._node_state.get(prefix_id)
            if prefix_info and (not prefix_info.loaded or created):
                return True
            parent_item = self._node_items[prefix_id]
            parent_id = prefix_id
        base_prefix = ""
        parent_info = self._node_state.get(parent_id)
        if parent_info and parent_info.node_type == "prefix":
            base_prefix = parent_info.prefix or ""
        self._remove_placeholder_children(parent_item)
        self._insert_file_node(parent_item, bucket, key, base_prefix)
        self._refresh_selection_controls()
        return True

    def _refresh_load_more_node(self, parent_item: QtGui.QStandardItem, listing: BucketListing) -> None:
        self._remove_load_more_nodes(parent_item)
        if listing.has_more and listing.continuation_token:
            self._insert_load_more_node(parent_item, listing)

    def _remove_load_more_nodes(self, parent_item: QtGui.QStandardItem) -> None:
        rows = list(range(parent_item.rowCount()))
        for row in reversed(rows):
            child = parent_item.child(row)
            if not child:
                continue
            node_id = child.data(NODE_ID_ROLE)
            node_info = self._node_state.get(node_id) if node_id else None
            if node_info and node_info.node_type == "load_more":
                self._delete_subtree(node_id)

    def _insert_load_more_node(self, parent_item: QtGui.QStandardItem, listing: BucketListing) -> None:
        node_id = f"load_more:{uuid.uuid4().hex}"
        item = QtGui.QStandardItem("Load more...")
        item.setEditable(False)
        self._register_node(
            node_id,
            item,
            NodeInfo(
                node_type="load_more",
                bucket=listing.name,
                prefix=listing.prefix,
                delimiter=listing.delimiter or None,
                continuation_token=listing.continuation_token,
                parent_id=parent_item.data(NODE_ID_ROLE),
            ),
        )
        parent_item.appendRow(item)

    def _relative_name(self, value: str, base_prefix: str) -> str:
        relative = value
        if base_prefix and value.startswith(base_prefix):
            relative = value[len(base_prefix) :]
        relative = relative.rstrip("/")
        if not relative:
            trimmed = value.rstrip("/")
            relative = trimmed or value
        return relative

    def _handle_tree_open(self, index: QtCore.QModelIndex) -> None:
        item = self._model.itemFromIndex(index)
        if not item:
            return
        node_id = item.data(NODE_ID_ROLE)
        if not node_id:
            return
        node_info = self._node_state.get(node_id)
        if not node_info or node_info.node_type != "prefix":
            return
        if node_info.loaded or node_info.loading:
            return
        node_info.loading = True

        def handle_success(listing: BucketListing) -> None:
            self._render_prefix_listing(node_id, listing)

        self.presenter.list_objects(
            bucket_name=node_info.bucket,
            max_keys=self._current_max_keys,
            prefix=node_info.prefix or "",
            on_success=handle_success,
            on_error=lambda msg: self._handle_prefix_error(node_id, msg),
        )

    def _handle_tree_double_click(self, index: QtCore.QModelIndex) -> None:
        item = self._model.itemFromIndex(index)
        if not item:
            return
        node_id = item.data(NODE_ID_ROLE)
        if not node_id:
            return
        node_info = self._node_state.get(node_id)
        if not node_info:
            return
        if node_info.node_type == "load_more":
            if node_info.loading or not node_info.continuation_token:
                return
            node_info.loading = True
            item.setText("Loading more...")

            def handle_success(listing: BucketListing) -> None:
                parent_id = node_info.parent_id
                if not parent_id:
                    return
                self._handle_load_more_result(node_id, parent_id, listing)

            self.presenter.list_objects(
                bucket_name=node_info.bucket,
                max_keys=self._current_max_keys,
                prefix=node_info.prefix or "",
                delimiter=node_info.delimiter,
                continuation_token=node_info.continuation_token,
                on_success=handle_success,
                on_error=lambda msg: self._handle_load_more_error(node_id, msg),
            )
        elif node_info.node_type == "object":
            self._show_object_details(node_info.bucket, node_info.key or "")

    def _handle_tree_right_click(self, pos: QtCore.QPoint) -> None:
        index = self.results_tree.indexAt(pos)
        if not index.isValid():
            return
        item = self._model.itemFromIndex(index)
        if not item:
            return
        node_id = item.data(NODE_ID_ROLE)
        if not node_id:
            return
        node_info = self._node_state.get(node_id)
        if not node_info:
            return
        selection_model = self.results_tree.selectionModel()
        if selection_model:
            if selection_model.isSelected(index):
                selection_model.setCurrentIndex(
                    index,
                    QtCore.QItemSelectionModel.NoUpdate | QtCore.QItemSelectionModel.Rows,
                )
            else:
                selection_model.setCurrentIndex(
                    index,
                    QtCore.QItemSelectionModel.ClearAndSelect | QtCore.QItemSelectionModel.Rows,
                )
        self._refresh_selection_controls()
        menu = None
        if node_info.node_type == "object":
            if len(self._get_selected_objects()) > 1:
                menu = self.object_multi_menu
            else:
                menu = self.object_menu
        elif node_info.node_type in {"prefix", "bucket"}:
            menu = self.folder_menu
        if not menu:
            return
        menu.exec(self.results_tree.viewport().mapToGlobal(pos))

    def _render_prefix_listing(self, node_id: str, listing: BucketListing) -> None:
        node_info = self._node_state.get(node_id)
        item = self._node_items.get(node_id)
        if not node_info or not item:
            return
        self._delete_child_nodes(item)
        if listing.error:
            item.appendRow(QtGui.QStandardItem(f"Error: {listing.error}"))
            node_info.loading = False
            return

        objects_added, prefixes_added = self._render_listing_contents(item, listing)
        if not (objects_added or prefixes_added):
            placeholder = "(Empty)"
            if node_info.node_type == "bucket":
                placeholder = "(No objects)"
            item.appendRow(QtGui.QStandardItem(placeholder))
        node_info.loaded = True
        node_info.loading = False
        prefix_label = listing.prefix or "/"
        self._set_status(
            f"Loaded {objects_added} object(s) and {prefixes_added} folder(s) under {prefix_label}."
        )

    def _handle_load_more_result(self, node_id: str, parent_id: str, listing: BucketListing) -> None:
        if node_id in self._node_items:
            self._delete_subtree(node_id)
        parent_item = self._node_items.get(parent_id)
        if not parent_item:
            return
        node_info = self._node_state.get(parent_id)
        objects_added, prefixes_added = self._render_listing_contents(parent_item, listing)
        if node_info and node_info.node_type == "prefix":
            node_info.loaded = True
            node_info.loading = False
        prefix_label = listing.prefix or "/"
        self._set_status(
            f"Loaded {objects_added} more object(s) and {prefixes_added} more folder(s) under {prefix_label}."
        )

    def _handle_load_more_error(self, node_id: str, message: str) -> None:
        node_info = self._node_state.get(node_id)
        item = self._node_items.get(node_id)
        if not node_info or not item:
            return
        node_info.loading = False
        item.setText("Load more...")
        self._show_error("List Error", f"Error loading more items: {message}")

    def _handle_prefix_error(self, node_id: str, message: str) -> None:
        node_info = self._node_state.get(node_id)
        item = self._node_items.get(node_id)
        if not node_info or not item:
            return
        self._delete_child_nodes(item)
        item.appendRow(QtGui.QStandardItem(f"Error: {message}"))
        node_info.loading = False
        prefix_label = node_info.prefix or "/"
        self._show_error("List Error", f"Error loading {prefix_label}: {message}")

    def _delete_child_nodes(self, parent_item: QtGui.QStandardItem) -> None:
        for row in reversed(range(parent_item.rowCount())):
            child = parent_item.child(row)
            if not child:
                continue
            node_id = child.data(NODE_ID_ROLE)
            if node_id:
                self._delete_subtree(node_id)
            else:
                parent_item.removeRow(row)

    def _delete_subtree(self, node_id: str) -> None:
        item = self._node_items.get(node_id)
        if not item:
            return
        self._remove_node_recursive(item)
        parent = item.parent() or self._model.invisibleRootItem()
        parent.removeRow(item.row())

    def _remove_node_recursive(self, item: QtGui.QStandardItem) -> None:
        for row in reversed(range(item.rowCount())):
            child = item.child(row)
            if not child:
                continue
            child_id = child.data(NODE_ID_ROLE)
            if child_id:
                self._remove_node_recursive(child)
            item.removeRow(row)
        node_id = item.data(NODE_ID_ROLE)
        if node_id:
            self._node_state.pop(node_id, None)
            self._node_items.pop(node_id, None)

    def _clear_tree(self) -> None:
        self._model.clear()
        self._node_state.clear()
        self._node_items.clear()

    def _set_status(self, message: str) -> None:
        self.status_label.setText(message)

    def _show_error(self, title: str, message: str) -> None:
        QtWidgets.QMessageBox.critical(self, title, message)
        self._set_status(message)

    def _start_transfer_dialog(self, *, title: str, description: str, total_bytes: int | None = None) -> TransferDialog:
        dialog = TransferDialog(self, title=title, description=description, total_bytes=total_bytes)
        self._transfer_dialog = dialog
        dialog.show()
        return dialog

    def _close_transfer_dialog(self, dialog: TransferDialog | None) -> None:
        if not dialog:
            return
        dialog.close()
        if self._transfer_dialog is dialog:
            self._transfer_dialog = None

    def _report_transfer_progress(self, dialog: TransferDialog, total: int) -> None:
        if not dialog:
            return
        dialog.update_progress(total)

    def _handle_transfer_cancelled(self, dialog: TransferDialog | None, message: str) -> None:
        self._close_transfer_dialog(dialog)
        self._set_status(message)

    def _show_object_details(self, bucket: str, key: str) -> None:
        dialog = ObjectDetailsDialog(
            self,
            bucket=bucket,
            key=key,
            on_download=lambda details=None: self._download_object(bucket, key, details),
            on_delete=lambda: self._delete_object(bucket, key),
            on_generate_url=lambda: self.open_signed_url_dialog(bucket=bucket, key=key),
        )

        def handle_success(details: ObjectDetails) -> None:
            dialog.display_details(details)

        def handle_error(message: str) -> None:
            dialog.display_error(message)

        self.presenter.get_object_details(
            bucket_name=bucket,
            key=key,
            on_success=handle_success,
            on_error=handle_error,
        )
        dialog.exec()

    def upload_file(self, *_: object) -> None:
        selection = self._get_selected_upload_target()
        if not selection:
            self._show_error("Error", "Please select a bucket or folder")
            return
        bucket, prefix = selection
        source_path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select file to upload")
        if not source_path:
            return
        self._upload_path_with_dialog(bucket, prefix, source_path)

    def _upload_path_with_dialog(self, bucket: str, prefix: str, source_path: str) -> None:
        try:
            source_size = os.path.getsize(source_path)
        except OSError:
            source_size = 0
        dialog = UploadDialog(
            self,
            bucket=bucket,
            source_path=source_path,
            source_size=source_size,
            initial_prefix=prefix,
        )
        result = dialog.exec_and_get()
        if not result:
            return
        key = result["key"]
        source_path = result["source_path"]
        if not self._confirm_overwrite_if_needed(bucket, key):
            return
        self._upload_object(bucket, key, source_path)

    def _upload_files_sequential(self, bucket: str, prefix: str, source_paths: list[str]) -> None:
        queue = [path for path in source_paths if path]
        if not queue:
            return
        cancelled = {"value": False}
        total_count = len(queue)

        def start_next() -> None:
            if cancelled["value"]:
                return
            if not queue:
                return
            source_path = queue.pop(0)
            filename = os.path.basename(source_path)
            try:
                key = compose_s3_key(prefix, filename)
            except ValueError:
                self._show_error("Upload Error", f"Cannot upload unnamed file: {source_path}")
                start_next()
                return
            if not self._confirm_overwrite_if_needed(bucket, key):
                start_next()
                return
            remaining = len(queue)
            position = total_count - remaining
            dialog = self._start_transfer_dialog(
                title="Uploading",
                description=f"Uploading {filename} ({position}/{total_count}) to s3://{bucket}/{key}",
            )

            def handle_success() -> None:
                self._close_transfer_dialog(dialog)
                if not self._add_object_to_tree(bucket, key):
                    self._schedule_object_refresh()
                self._set_status(f"Uploaded {key} to {bucket}.")

            def handle_error(message: str) -> None:
                self._close_transfer_dialog(dialog)
                self._show_error("Upload Error", f"Error uploading {key}: {message}")

            def handle_cancelled(message: str) -> None:
                cancelled["value"] = True
                queue.clear()
                self._handle_transfer_cancelled(dialog, message)

            self.presenter.upload_object(
                bucket_name=bucket,
                key=key,
                source_path=source_path,
                multipart_threshold=self._settings.upload_multipart_threshold,
                multipart_chunk_size=self._settings.upload_chunk_size,
                max_concurrency=self._settings.upload_max_concurrency,
                on_progress=lambda total: self._report_transfer_progress(dialog, total),
                cancel_requested=dialog.cancel_requested,
                on_success=handle_success,
                on_error=handle_error,
                on_cancelled=handle_cancelled,
                on_done=start_next,
            )

        start_next()

    def _confirm_overwrite_if_needed(self, bucket: str, key: str) -> bool:
        if self._find_node(node_type="object", bucket=bucket, key=key):
            confirm = QtWidgets.QMessageBox.question(
                self,
                "Overwrite Object",
                f"s3://{bucket}/{key} already exists. Overwrite?",
            )
            return confirm == QtWidgets.QMessageBox.Yes
        return True

    def _download_object(self, bucket: str, key: str, details: ObjectDetails | None = None) -> None:
        filename = key.rsplit("/", 1)[-1]
        destination, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Save file", filename)
        if not destination:
            return
        size_value = details.size if details else None
        dialog = self._start_transfer_dialog(
            title="Downloading",
            description=f"Downloading s3://{bucket}/{key}",
            total_bytes=size_value,
        )

        def handle_success() -> None:
            self._close_transfer_dialog(dialog)
            self._set_status(f"Downloaded {key} to {destination}.")

        def handle_error(message: str) -> None:
            self._close_transfer_dialog(dialog)
            self._show_error("Download Error", f"Error downloading {key}: {message}")

        def handle_cancelled(message: str) -> None:
            self._handle_transfer_cancelled(dialog, message)

        self.presenter.download_object(
            bucket_name=bucket,
            key=key,
            destination=destination,
            on_progress=lambda total: self._report_transfer_progress(dialog, total),
            cancel_requested=dialog.cancel_requested,
            on_success=handle_success,
            on_error=handle_error,
            on_cancelled=handle_cancelled,
        )

    def _download_objects_sequential(self, objects: list[tuple[str, str]], target_dir: str) -> None:
        queue = [(bucket, key) for bucket, key in objects if key]
        if not queue:
            return
        cancelled = {"value": False}
        total_count = len(queue)
        planned_paths: set[str] = set()

        def start_next() -> None:
            if cancelled["value"]:
                return
            if not queue:
                self._set_status(f"Downloaded {total_count} object(s) to {target_dir}.")
                return
            bucket, key = queue.pop(0)
            filename = key.rsplit("/", 1)[-1] or "download"
            destination = self._unique_download_path(target_dir, filename, planned_paths)
            planned_paths.add(destination)
            position = total_count - len(queue)
            dialog = self._start_transfer_dialog(
                title="Downloading",
                description=f"Downloading {position}/{total_count}: s3://{bucket}/{key}",
            )

            def handle_success() -> None:
                self._close_transfer_dialog(dialog)
                self._set_status(f"Downloaded {key} to {destination}.")

            def handle_error(message: str) -> None:
                self._close_transfer_dialog(dialog)
                self._show_error("Download Error", f"Error downloading {key}: {message}")

            def handle_cancelled(message: str) -> None:
                cancelled["value"] = True
                queue.clear()
                self._handle_transfer_cancelled(dialog, message)

            self.presenter.download_object(
                bucket_name=bucket,
                key=key,
                destination=destination,
                on_progress=lambda total: self._report_transfer_progress(dialog, total),
                cancel_requested=dialog.cancel_requested,
                on_success=handle_success,
                on_error=handle_error,
                on_cancelled=handle_cancelled,
                on_done=start_next,
            )

        start_next()

    def _unique_download_path(self, target_dir: str, filename: str, planned_paths: set[str]) -> str:
        base, extension = os.path.splitext(filename)
        candidate = os.path.join(target_dir, filename)
        counter = 1
        while candidate in planned_paths or os.path.exists(candidate):
            candidate = os.path.join(target_dir, f"{base} ({counter}){extension}")
            counter += 1
        return candidate

    def _delete_object(self, bucket: str, key: str) -> None:
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Delete Object",
            f"Delete s3://{bucket}/{key}?",
        )
        if confirm != QtWidgets.QMessageBox.Yes:
            return

        def handle_success() -> None:
            if not self._remove_object_from_tree(bucket, key):
                self._schedule_object_refresh()
            self._set_status(f"Deleted {key}.")

        def handle_error(message: str) -> None:
            self._show_error("Delete Error", f"Error deleting {key}: {message}")

        self.presenter.delete_object(
            bucket_name=bucket,
            key=key,
            on_success=handle_success,
            on_error=handle_error,
        )

    def _delete_objects_sequential(self, objects: list[tuple[str, str]]) -> None:
        queue = [(bucket, key) for bucket, key in objects if key]
        if not queue:
            return
        total_count = len(queue)

        def start_next() -> None:
            if not queue:
                self._set_status(f"Deleted {total_count} object(s).")
                return
            bucket, key = queue.pop(0)

            def handle_success() -> None:
                if not self._remove_object_from_tree(bucket, key):
                    self._schedule_object_refresh()
                start_next()

            def handle_error(message: str) -> None:
                self._show_error("Delete Error", f"Error deleting {key}: {message}")
                start_next()

            self.presenter.delete_object(
                bucket_name=bucket,
                key=key,
                on_success=handle_success,
                on_error=handle_error,
            )

        start_next()

    def _upload_object(self, bucket: str, key: str, source_path: str) -> None:
        dialog = self._start_transfer_dialog(
            title="Uploading",
            description=f"Uploading {os.path.basename(source_path)} to s3://{bucket}/{key}",
        )

        def handle_success() -> None:
            self._close_transfer_dialog(dialog)
            if not self._add_object_to_tree(bucket, key):
                self._schedule_object_refresh()
            self._set_status(f"Uploaded {key} to {bucket}.")

        def handle_error(message: str) -> None:
            self._close_transfer_dialog(dialog)
            self._show_error("Upload Error", f"Error uploading {key}: {message}")

        def handle_cancelled(message: str) -> None:
            self._handle_transfer_cancelled(dialog, message)

        self.presenter.upload_object(
            bucket_name=bucket,
            key=key,
            source_path=source_path,
            multipart_threshold=self._settings.upload_multipart_threshold,
            multipart_chunk_size=self._settings.upload_chunk_size,
            max_concurrency=self._settings.upload_max_concurrency,
            on_progress=lambda total: self._report_transfer_progress(dialog, total),
            cancel_requested=dialog.cancel_requested,
            on_success=handle_success,
            on_error=handle_error,
            on_cancelled=handle_cancelled,
        )

    def open_signed_url_dialog(
        self,
        bucket: str | None = None,
        key: str | None = None,
        *_: object,
    ) -> None:
        bucket_name = bucket or self._selected_bucket
        if not bucket_name:
            self._show_error("Error", "Please select a bucket")
            return
        key_value = key or ""
        dialog = SignedUrlDialog(
            self,
            bucket=bucket_name,
            key=key_value,
            default_max_size=self._settings.default_post_max_size,
        )

        def handle_success(result: str | dict[str, dict[str, str] | str]) -> None:
            dialog.display_result(result)

        def handle_error(message: str) -> None:
            dialog.display_error(message)

        dialog.generate_requested.connect(
            lambda payload: self.presenter.generate_presigned_url(
                bucket_name=payload["bucket"],
                key=payload["key"],
                method=payload["method"],
                expires_in=payload["expires_in"],
                content_type=payload["content_type"],
                content_disposition=payload["content_disposition"],
                post_key_mode=payload["post_key_mode"],
                max_size=payload["max_size"],
                on_success=handle_success,
                on_error=handle_error,
            )
        )
        dialog.exec()


class TransferDialog(QtWidgets.QDialog):
    """Modal dialog that displays transfer progress and offers cancellation."""

    def __init__(self, parent: QtWidgets.QWidget, *, title: str, description: str, total_bytes: int | None = None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self._total_bytes = total_bytes or 0
        self._indeterminate = not total_bytes or total_bytes <= 0
        self._transferred = 0
        self._cancel_requested = False

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        desc_label = QtWidgets.QLabel(description)
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)

        self.progress = QtWidgets.QProgressBar(self)
        if self._indeterminate:
            self.progress.setRange(0, 0)
        else:
            self.progress.setRange(0, max(self._total_bytes, 1))
            self.progress.setValue(0)
        layout.addWidget(self.progress)

        self.progress_label = QtWidgets.QLabel("Preparing transfer...")
        layout.addWidget(self.progress_label)

        self.status_label = QtWidgets.QLabel("In progress...")
        palette = self.status_label.palette()
        palette.setColor(QtGui.QPalette.WindowText, QtGui.QColor("gray"))
        self.status_label.setPalette(palette)
        layout.addWidget(self.status_label)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        self.cancel_button = QtWidgets.QPushButton("Cancel")
        self.cancel_button.clicked.connect(self._on_cancel)
        button_row.addWidget(self.cancel_button)
        layout.addLayout(button_row)

    def update_progress(self, transferred: int) -> None:
        self._transferred = max(transferred, 0)
        if self._indeterminate:
            self.progress_label.setText(f"{format_size(self._transferred)} transferred")
        else:
            maximum = max(self._total_bytes, 1)
            percent = min(self._transferred / maximum, 1.0)
            self.progress.setValue(min(self._transferred, maximum))
            total_label = format_size(self._total_bytes)
            self.progress_label.setText(
                f"{format_size(self._transferred)} of {total_label} ({percent:.0%})"
            )
        if not self._cancel_requested:
            self.status_label.setText("Transferring...")

    def cancel_requested(self) -> bool:
        return self._cancel_requested

    def set_status(self, message: str) -> None:
        self.status_label.setText(message)

    def _on_cancel(self) -> None:
        if self._cancel_requested:
            return
        self._cancel_requested = True
        self.status_label.setText("Cancelling...")
        self.cancel_button.setEnabled(False)


class ConnectionDialog(QtWidgets.QDialog):
    """Modal dialog for creating or editing connection profiles."""

    def __init__(
        self,
        parent: QtWidgets.QWidget,
        *,
        title: str,
        profile: ConnectionProfile | None = None,
        primary_action: str = "save",
        primary_label: str | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.result: dict | None = None
        self.original_name = profile.name if profile else None
        self._primary_action = primary_action or "save"
        if primary_label:
            self._save_label = primary_label
        elif self._primary_action == "save":
            self._save_label = "Save"
        else:
            self._save_label = self._primary_action.replace("_", " ").title()
        self._connect_on_save = self._primary_action == "save_and_connect"
        self._original_values = (
            {
                "name": profile.name,
                "endpoint_url": profile.endpoint_url,
                "access_key": profile.access_key,
                "secret_key": profile.secret_key,
            }
            if profile
            else None
        )

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.name_edit = QtWidgets.QLineEdit(profile.name if profile else "")
        self.endpoint_edit = QtWidgets.QLineEdit(profile.endpoint_url if profile else "")
        self.access_key_edit = QtWidgets.QLineEdit(profile.access_key if profile else "")
        self.secret_key_edit = QtWidgets.QLineEdit(profile.secret_key if profile else "")
        self.secret_key_edit.setEchoMode(QtWidgets.QLineEdit.Password)

        form.addRow("Name:", self.name_edit)
        form.addRow("Endpoint URL:", self.endpoint_edit)
        form.addRow("Access Key ID:", self.access_key_edit)
        form.addRow("Secret Access Key:", self.secret_key_edit)
        layout.addLayout(form)

        buttons = QtWidgets.QHBoxLayout()
        buttons.addStretch(1)
        self.primary_button = QtWidgets.QPushButton(self._save_label)
        self.primary_button.clicked.connect(self._on_save)
        buttons.addWidget(self.primary_button)
        cancel_button = QtWidgets.QPushButton("Cancel")
        cancel_button.clicked.connect(self._on_cancel)
        buttons.addWidget(cancel_button)
        if profile:
            delete_button = QtWidgets.QPushButton("Delete")
            delete_button.clicked.connect(self._on_delete)
            buttons.addWidget(delete_button)
        layout.addLayout(buttons)

        for edit in (self.name_edit, self.endpoint_edit, self.access_key_edit, self.secret_key_edit):
            edit.textChanged.connect(self._update_primary_state)
        self._update_primary_state()

    def exec_and_get(self) -> dict | None:
        if self.exec() != QtWidgets.QDialog.Accepted:
            return None
        return self.result

    def _fields_filled(self) -> bool:
        return all(
            [
                self.name_edit.text().strip(),
                self.endpoint_edit.text().strip(),
                self.access_key_edit.text().strip(),
                self.secret_key_edit.text().strip(),
            ]
        )

    def _has_changes(self) -> bool:
        if not self._original_values:
            return True
        current = {
            "name": self.name_edit.text().strip(),
            "endpoint_url": self.endpoint_edit.text().strip(),
            "access_key": self.access_key_edit.text().strip(),
            "secret_key": self.secret_key_edit.text().strip(),
        }
        return current != self._original_values

    def _resolve_primary_action(self) -> str:
        if self._connect_on_save and self._original_values and not self._has_changes():
            return "connect"
        return self._primary_action

    def _resolve_primary_label(self) -> str:
        if self._connect_on_save and self._original_values and not self._has_changes():
            return "Connect"
        return self._save_label

    def _update_primary_state(self) -> None:
        label = self._resolve_primary_label()
        enabled = self._fields_filled()
        self.primary_button.setText(label)
        self.primary_button.setEnabled(enabled)

    def _on_save(self) -> None:
        if not self._fields_filled():
            QtWidgets.QMessageBox.critical(self, "Error", "All fields are required")
            return
        action = self._resolve_primary_action()
        if action == "connect":
            target_name = self.original_name or self.name_edit.text().strip()
            self.result = {"action": "connect", "name": target_name}
        else:
            profile = ConnectionProfile(
                name=self.name_edit.text().strip(),
                endpoint_url=self.endpoint_edit.text().strip(),
                access_key=self.access_key_edit.text().strip(),
                secret_key=self.secret_key_edit.text().strip(),
            )
            self.result = {"action": action, "profile": profile, "original_name": self.original_name}
        self.accept()

    def _on_delete(self) -> None:
        if not self.original_name:
            return
        confirmed = QtWidgets.QMessageBox.question(
            self,
            "Delete Connection",
            f"Delete connection '{self.original_name}'?",
        )
        if confirmed != QtWidgets.QMessageBox.Yes:
            return
        self.result = {"action": "delete", "name": self.original_name}
        self.accept()

    def _on_cancel(self) -> None:
        self.result = None
        self.reject()


class ObjectDetailsDialog(QtWidgets.QDialog):
    """Modal dialog that loads and displays metadata for an object."""

    def __init__(
        self,
        parent: QtWidgets.QWidget,
        *,
        bucket: str,
        key: str,
        on_download: Callable[[ObjectDetails | None], None] | None = None,
        on_delete: Callable[[], None] | None = None,
        on_generate_url: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Object Details")
        self.setModal(True)
        self._details: ObjectDetails | None = None
        self._on_download = on_download
        self._on_delete = on_delete
        self._on_generate_url = on_generate_url

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.path_field = QtWidgets.QLineEdit(f"s3://{bucket}/{key}")
        self.path_field.setReadOnly(True)
        form.addRow("Path:", self.path_field)
        layout.addLayout(form)

        self.status_label = QtWidgets.QLabel("Loading metadata...")
        layout.addWidget(self.status_label)

        self.progress = QtWidgets.QProgressBar(self)
        self.progress.setRange(0, 0)
        layout.addWidget(self.progress)

        self.details_group = QtWidgets.QGroupBox("Details")
        details_layout = QtWidgets.QFormLayout(self.details_group)
        self._detail_fields = {}
        for label in ["Bucket", "Key", "Size", "Last modified", "Storage class", "ETag", "Content type"]:
            field = QtWidgets.QLineEdit("-")
            field.setReadOnly(True)
            details_layout.addRow(f"{label}:", field)
            self._detail_fields[label] = field
        layout.addWidget(self.details_group)
        self.details_group.setVisible(False)

        self.checksums_text = QtWidgets.QPlainTextEdit()
        self.checksums_text.setReadOnly(True)
        layout.addWidget(QtWidgets.QLabel("Checksums:"))
        layout.addWidget(self.checksums_text)

        self.metadata_text = QtWidgets.QPlainTextEdit()
        self.metadata_text.setReadOnly(True)
        layout.addWidget(QtWidgets.QLabel("Metadata:"))
        layout.addWidget(self.metadata_text)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        if self._on_download:
            download_button = QtWidgets.QPushButton("Download")
            download_button.clicked.connect(self._handle_download)
            button_row.addWidget(download_button)
        if self._on_delete:
            delete_button = QtWidgets.QPushButton("Delete")
            delete_button.clicked.connect(self._handle_delete)
            button_row.addWidget(delete_button)
        if self._on_generate_url:
            url_button = QtWidgets.QPushButton("Signed URL")
            url_button.clicked.connect(self._handle_signed_url)
            button_row.addWidget(url_button)
        close_button = QtWidgets.QPushButton("Close")
        close_button.clicked.connect(self.reject)
        button_row.addWidget(close_button)
        layout.addLayout(button_row)

    def display_details(self, details: ObjectDetails) -> None:
        self._details = details
        self.progress.setVisible(False)
        self.details_group.setVisible(True)
        self.status_label.setText("Metadata loaded.")
        self._detail_fields["Bucket"].setText(details.bucket)
        self._detail_fields["Key"].setText(details.key)
        self._detail_fields["Size"].setText(format_size(details.size))
        self._detail_fields["Last modified"].setText(format_last_modified(details.last_modified))
        self._detail_fields["Storage class"].setText(details.storage_class or "-")
        self._detail_fields["ETag"].setText(details.etag or "-")
        self._detail_fields["Content type"].setText(details.content_type or "-")
        checksums_value = "\n".join(f"{k}: {v}" for k, v in sorted(details.checksums.items())) or "None"
        self.checksums_text.setPlainText(checksums_value)
        metadata_value = "\n".join(f"{k}: {v}" for k, v in sorted(details.metadata.items())) or "None"
        self.metadata_text.setPlainText(metadata_value)

    def display_error(self, message: str) -> None:
        self.progress.setVisible(False)
        self.details_group.setVisible(False)
        self.status_label.setText(f"Error loading metadata: {message}")

    def _handle_download(self) -> None:
        if not self._on_download:
            return
        self._on_download(self._details)

    def _handle_delete(self) -> None:
        if self._on_delete:
            self._on_delete()

    def _handle_signed_url(self) -> None:
        if self._on_generate_url:
            self._on_generate_url()


class UploadDialog(QtWidgets.QDialog):
    """Dialog to confirm upload destination details."""

    def __init__(
        self,
        parent: QtWidgets.QWidget,
        *,
        bucket: str,
        source_path: str,
        source_size: int,
        initial_prefix: str = "",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Upload File")
        self.setModal(True)
        self._bucket = bucket
        self._source_path = source_path
        self._source_size = source_size
        self._result: dict | None = None

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        bucket_label = QtWidgets.QLabel(f"s3://{bucket}")
        form.addRow("Bucket:", bucket_label)

        source_field = QtWidgets.QLineEdit(source_path)
        source_field.setReadOnly(True)
        form.addRow("Source file:", source_field)

        form.addRow("File size:", QtWidgets.QLabel(format_size(source_size)))

        self.prefix_edit = QtWidgets.QLineEdit(initial_prefix or "")
        form.addRow("Destination folder:", self.prefix_edit)

        default_name = os.path.basename(source_path)
        self.name_edit = QtWidgets.QLineEdit(default_name)
        form.addRow("Object name:", self.name_edit)

        self.full_path_label = QtWidgets.QLabel("")
        form.addRow("Resulting path:", self.full_path_label)

        layout.addLayout(form)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        upload_button = QtWidgets.QPushButton("Upload")
        upload_button.clicked.connect(self._on_upload)
        button_row.addWidget(upload_button)
        cancel_button = QtWidgets.QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_row.addWidget(cancel_button)
        layout.addLayout(button_row)

        self.prefix_edit.textChanged.connect(self._update_full_path)
        self.name_edit.textChanged.connect(self._update_full_path)
        self._update_full_path()

    def exec_and_get(self) -> dict | None:
        if self.exec() != QtWidgets.QDialog.Accepted:
            return None
        return self._result

    def _update_full_path(self) -> None:
        try:
            path = compose_s3_key(self.prefix_edit.text(), self.name_edit.text())
        except ValueError:
            path = ""
        self.full_path_label.setText(path)

    def _on_upload(self) -> None:
        try:
            key = compose_s3_key(self.prefix_edit.text(), self.name_edit.text())
        except ValueError:
            QtWidgets.QMessageBox.critical(self, "Error", "Object name cannot be empty")
            return
        self._result = {"bucket": self._bucket, "key": key, "source_path": self._source_path}
        self.accept()


class UploadMultipleDialog(QtWidgets.QDialog):
    """Dialog to confirm upload destination for multiple files."""

    def __init__(
        self,
        parent: QtWidgets.QWidget,
        *,
        bucket: str,
        file_count: int,
        initial_prefix: str = "",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Upload Files")
        self.setModal(True)
        self._result: dict | None = None

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        bucket_label = QtWidgets.QLabel(f"s3://{bucket}")
        form.addRow("Bucket:", bucket_label)

        form.addRow("Files to upload:", QtWidgets.QLabel(str(file_count)))

        self.prefix_edit = QtWidgets.QLineEdit(initial_prefix or "")
        form.addRow("Destination folder:", self.prefix_edit)

        layout.addLayout(form)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        upload_button = QtWidgets.QPushButton("Upload")
        upload_button.clicked.connect(self._on_upload)
        button_row.addWidget(upload_button)
        cancel_button = QtWidgets.QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_row.addWidget(cancel_button)
        layout.addLayout(button_row)

    def exec_and_get(self) -> dict | None:
        if self.exec() != QtWidgets.QDialog.Accepted:
            return None
        return self._result

    def _on_upload(self) -> None:
        self._result = {"prefix": self.prefix_edit.text()}
        self.accept()


class SignedUrlDialog(QtWidgets.QDialog):
    """Dialog for generating signed URLs."""

    generate_requested = QtCore.Signal(dict)

    def __init__(self, parent: QtWidgets.QWidget, *, bucket: str, key: str, default_max_size: int) -> None:
        super().__init__(parent)
        self.setWindowTitle("Signed URL")
        self.setModal(True)
        self._bucket = bucket
        self._default_max_size = default_max_size
        self._post_fields: dict[str, str] | None = None

        layout = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.bucket_field = QtWidgets.QLineEdit(bucket)
        self.bucket_field.setReadOnly(True)
        form.addRow("Bucket:", self.bucket_field)

        self.key_edit = QtWidgets.QLineEdit(key)
        form.addRow("Object key:", self.key_edit)

        self.full_path_label = QtWidgets.QLabel("")
        form.addRow("Full path:", self.full_path_label)

        method_layout = QtWidgets.QHBoxLayout()
        self.method_group = QtWidgets.QButtonGroup(self)
        for label, value in [("GET", "get"), ("PUT", "put"), ("POST", "post")]:
            button = QtWidgets.QRadioButton(label)
            self.method_group.addButton(button)
            button.setProperty("method", value)
            method_layout.addWidget(button)
            if value == "get":
                button.setChecked(True)
        form.addRow("Operation:", method_layout)

        self.post_mode_group = QtWidgets.QButtonGroup(self)
        post_layout = QtWidgets.QHBoxLayout()
        post_single = QtWidgets.QRadioButton("Single file")
        post_single.setProperty("post_mode", "single")
        post_prefix = QtWidgets.QRadioButton("Key prefix")
        post_prefix.setProperty("post_mode", "prefix")
        post_single.setChecked(True)
        self.post_mode_group.addButton(post_single)
        self.post_mode_group.addButton(post_prefix)
        post_layout.addWidget(post_single)
        post_layout.addWidget(post_prefix)
        form.addRow("POST key mode:", post_layout)

        size_value, size_unit = split_size_bytes(default_max_size)
        self.max_size_edit = QtWidgets.QLineEdit(size_value)
        self.max_size_unit = QtWidgets.QComboBox()
        self.max_size_unit.addItems(["B", "KB", "MB", "GB"])
        if size_unit in ["B", "KB", "MB", "GB"]:
            self.max_size_unit.setCurrentText(size_unit)
        size_layout = QtWidgets.QHBoxLayout()
        size_layout.addWidget(self.max_size_edit)
        size_layout.addWidget(self.max_size_unit)
        form.addRow("Max file size:", size_layout)

        self.expires_edit = QtWidgets.QLineEdit("3600")
        form.addRow("Expiration (seconds):", self.expires_edit)

        self.content_type_edit = QtWidgets.QLineEdit("")
        form.addRow("Content-Type (optional):", self.content_type_edit)

        self.content_disp_edit = QtWidgets.QLineEdit("")
        form.addRow("Content-Disposition (optional):", self.content_disp_edit)

        layout.addLayout(form)

        self.status_label = QtWidgets.QLabel("")
        layout.addWidget(self.status_label)

        self.url_text = QtWidgets.QPlainTextEdit()
        self.url_text.setReadOnly(True)
        layout.addWidget(QtWidgets.QLabel("Signed URL:"))
        layout.addWidget(self.url_text)

        self.wget_text = QtWidgets.QPlainTextEdit()
        self.wget_text.setReadOnly(True)
        layout.addWidget(QtWidgets.QLabel("wget command:"))
        layout.addWidget(self.wget_text)

        self.curl_text = QtWidgets.QPlainTextEdit()
        self.curl_text.setReadOnly(True)
        layout.addWidget(QtWidgets.QLabel("curl command:"))
        layout.addWidget(self.curl_text)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        generate_button = QtWidgets.QPushButton("Generate")
        generate_button.clicked.connect(self._on_generate)
        button_row.addWidget(generate_button)
        close_button = QtWidgets.QPushButton("Close")
        close_button.clicked.connect(self.reject)
        button_row.addWidget(close_button)
        layout.addLayout(button_row)

        self.key_edit.textChanged.connect(self._update_full_path)
        self.method_group.buttonToggled.connect(lambda *_: self._toggle_post_options())
        self._update_full_path()
        self._toggle_post_options()

    def _update_full_path(self) -> None:
        key = self.key_edit.text().strip()
        value = f"s3://{self._bucket}/{key}" if key else f"s3://{self._bucket}"
        self.full_path_label.setText(value)

    def _toggle_post_options(self) -> None:
        method = self._current_method()
        is_post = method == "post"
        self.max_size_edit.setEnabled(is_post)
        self.max_size_unit.setEnabled(is_post)
        for button in self.post_mode_group.buttons():
            button.setEnabled(is_post)

    def _current_method(self) -> str:
        for button in self.method_group.buttons():
            if button.isChecked():
                return button.property("method")
        return "get"

    def _current_post_mode(self) -> str:
        for button in self.post_mode_group.buttons():
            if button.isChecked():
                return button.property("post_mode")
        return "single"

    def _on_generate(self) -> None:
        key = self.key_edit.text().strip()
        if not key:
            QtWidgets.QMessageBox.critical(self, "Error", "Object key is required")
            return
        try:
            expires_in = int(self.expires_edit.text().strip())
        except ValueError:
            QtWidgets.QMessageBox.critical(self, "Error", "Expiration must be a number")
            return
        max_size = None
        if self._current_method() == "post":
            max_size = parse_size_bytes(self.max_size_edit.text(), self.max_size_unit.currentText())
            if max_size is None:
                QtWidgets.QMessageBox.critical(self, "Error", "Max file size must be valid")
                return
        payload = {
            "bucket": self._bucket,
            "key": key,
            "method": self._current_method(),
            "expires_in": expires_in,
            "content_type": self.content_type_edit.text().strip() or None,
            "content_disposition": self.content_disp_edit.text().strip() or None,
            "post_key_mode": self._current_post_mode(),
            "max_size": max_size,
        }
        self.status_label.setText("Generating signed URL...")
        self.generate_requested.emit(payload)

    def display_result(self, result: str | dict[str, dict[str, str] | str]) -> None:
        if isinstance(result, dict):
            url = result.get("url", "")
            post_fields = result.get("fields", {}) if isinstance(result.get("fields"), dict) else {}
        else:
            url = result
            post_fields = None
        self._post_fields = post_fields
        self.status_label.setText("Signed URL generated.")
        self.url_text.setPlainText(url)
        self._display_commands(url, post_fields)

    def display_error(self, message: str) -> None:
        self.status_label.setText(f"Error generating URL: {message}")

    def _display_commands(self, url: str, post_fields: dict[str, str] | None) -> None:
        method = self._current_method()
        filename = suggest_command_filename(self.key_edit.text())
        wget_cmd, curl_cmd = build_signed_url_commands(
            method=method,
            url=url,
            filename=filename,
            content_type=self.content_type_edit.text().strip() or None,
            content_disposition=self.content_disp_edit.text().strip() or None,
            post_fields=post_fields,
        )
        self.wget_text.setPlainText(wget_cmd or "")
        self.curl_text.setPlainText(curl_cmd or "")


class SettingsDialog(QtWidgets.QDialog):
    """Dialog for editing application settings."""

    def __init__(self, parent: QtWidgets.QWidget, *, settings: AppSettings) -> None:
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.result_settings: AppSettings | None = None
        self._existing_settings = settings

        layout = QtWidgets.QVBoxLayout(self)
        tabs = QtWidgets.QTabWidget(self)

        bucket_tab = QtWidgets.QWidget()
        bucket_layout = QtWidgets.QFormLayout(bucket_tab)
        self.fetch_limit_edit = QtWidgets.QLineEdit(str(settings.fetch_limit))
        bucket_layout.addRow("Fetch limit:", self.fetch_limit_edit)
        self.remember_checkbox = QtWidgets.QCheckBox("Remember last bucket/connection")
        self.remember_checkbox.setChecked(settings.remember_last_bucket)
        bucket_layout.addRow(self.remember_checkbox)

        signed_tab = QtWidgets.QWidget()
        signed_layout = QtWidgets.QFormLayout(signed_tab)
        default_value, default_unit = split_size_bytes(settings.default_post_max_size)
        self.default_size_edit = QtWidgets.QLineEdit(default_value)
        self.default_size_unit = QtWidgets.QComboBox()
        self.default_size_unit.addItems(["B", "KB", "MB", "GB"])
        self.default_size_unit.setCurrentText(default_unit)
        default_layout = QtWidgets.QHBoxLayout()
        default_layout.addWidget(self.default_size_edit)
        default_layout.addWidget(self.default_size_unit)
        signed_layout.addRow("Default POST max size:", default_layout)

        upload_tab = QtWidgets.QWidget()
        upload_layout = QtWidgets.QFormLayout(upload_tab)
        threshold_value, threshold_unit = split_size_bytes(settings.upload_multipart_threshold)
        chunk_value, chunk_unit = split_size_bytes(settings.upload_chunk_size)

        self.threshold_edit = QtWidgets.QLineEdit(threshold_value)
        self.threshold_unit = QtWidgets.QComboBox()
        self.threshold_unit.addItems(["B", "KB", "MB", "GB"])
        self.threshold_unit.setCurrentText(threshold_unit)
        threshold_layout = QtWidgets.QHBoxLayout()
        threshold_layout.addWidget(self.threshold_edit)
        threshold_layout.addWidget(self.threshold_unit)
        upload_layout.addRow("Upload multipart threshold:", threshold_layout)

        self.chunk_edit = QtWidgets.QLineEdit(chunk_value)
        self.chunk_unit = QtWidgets.QComboBox()
        self.chunk_unit.addItems(["B", "KB", "MB", "GB"])
        self.chunk_unit.setCurrentText(chunk_unit)
        chunk_layout = QtWidgets.QHBoxLayout()
        chunk_layout.addWidget(self.chunk_edit)
        chunk_layout.addWidget(self.chunk_unit)
        upload_layout.addRow("Upload chunk size:", chunk_layout)

        self.concurrency_edit = QtWidgets.QLineEdit(str(settings.upload_max_concurrency))
        upload_layout.addRow("Upload max concurrency:", self.concurrency_edit)

        tabs.addTab(bucket_tab, "Bucket")
        tabs.addTab(signed_tab, "Signed URL")
        tabs.addTab(upload_tab, "Upload")
        layout.addWidget(tabs)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        save_button = QtWidgets.QPushButton("Save")
        save_button.clicked.connect(self._on_save)
        button_row.addWidget(save_button)
        cancel_button = QtWidgets.QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_row.addWidget(cancel_button)
        layout.addLayout(button_row)

    def _on_save(self) -> None:
        try:
            fetch_limit = int(self.fetch_limit_edit.text().strip())
        except ValueError:
            QtWidgets.QMessageBox.critical(self, "Error", "Fetch limit must be a whole number")
            return
        if fetch_limit <= 0:
            QtWidgets.QMessageBox.critical(self, "Error", "Fetch limit must be greater than zero")
            return
        default_post_max_size = parse_size_bytes(self.default_size_edit.text(), self.default_size_unit.currentText())
        if default_post_max_size is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Default POST max size must be valid")
            return
        multipart_threshold = parse_size_bytes(self.threshold_edit.text(), self.threshold_unit.currentText())
        if multipart_threshold is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Upload multipart threshold must be valid")
            return
        chunk_size = parse_size_bytes(self.chunk_edit.text(), self.chunk_unit.currentText())
        if chunk_size is None:
            QtWidgets.QMessageBox.critical(self, "Error", "Upload chunk size must be valid")
            return
        try:
            max_concurrency = int(self.concurrency_edit.text().strip())
        except ValueError:
            QtWidgets.QMessageBox.critical(self, "Error", "Upload max concurrency must be a whole number")
            return
        if max_concurrency <= 0:
            QtWidgets.QMessageBox.critical(self, "Error", "Upload max concurrency must be greater than zero")
            return

        self.result_settings = AppSettings(
            fetch_limit=fetch_limit,
            default_post_max_size=default_post_max_size,
            upload_multipart_threshold=multipart_threshold,
            upload_chunk_size=chunk_size,
            upload_max_concurrency=max_concurrency,
            remember_last_bucket=self.remember_checkbox.isChecked(),
            last_bucket=self._existing_settings.last_bucket,
            last_connection=self._existing_settings.last_connection,
        )
        self.accept()


class AboutDialog(QtWidgets.QDialog):
    """Dialog displaying package metadata."""

    def __init__(self, parent: QtWidgets.QWidget, *, package_info: PackageInfo) -> None:
        super().__init__(parent)
        self.setWindowTitle("About")
        self.setModal(True)

        layout = QtWidgets.QVBoxLayout(self)
        title = QtWidgets.QLabel(f"{package_info.name} {package_info.version}")
        title_font = title.font()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        summary = QtWidgets.QLabel(package_info.summary or "")
        summary.setAlignment(QtCore.Qt.AlignCenter)
        summary.setWordWrap(True)
        layout.addWidget(summary)

        if package_info.author:
            author = QtWidgets.QLabel(f"Author: {package_info.author}")
            author.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(author)
        if package_info.homepage:
            homepage = QtWidgets.QLabel(f"Homepage: {package_info.homepage}")
            homepage.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(homepage)
        if package_info.repository:
            repo = QtWidgets.QLabel(f"Repository: {package_info.repository}")
            repo.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(repo)

        close_button = QtWidgets.QPushButton("Close")
        close_button.clicked.connect(self.accept)
        layout.addWidget(close_button, alignment=QtCore.Qt.AlignCenter)
