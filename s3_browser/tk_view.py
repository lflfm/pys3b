from __future__ import annotations
"""Tkinter-based UI for the S3 browser application."""
import os
import threading
import tkinter as tk
from dataclasses import dataclass
from importlib.metadata import version, metadata, PackageNotFoundError
from tkinter import filedialog, messagebox, ttk
from typing import Callable

from botocore.exceptions import BotoCoreError, ClientError

from .controller import S3BrowserController
from .models import BucketListing, ObjectDetails
from .profiles import ConnectionProfile
from .services import TransferCancelledError
from .settings import AppSettings, SettingsStorage

DIST_NAME = "pys3b"
SIZE_UNITS = ("B", "KB", "MB")
SIZE_UNIT_FACTORS = {"B": 1, "KB": 1024, "MB": 1024 * 1024}

@dataclass(frozen=True)
class PackageInfo:
    name: str
    version: str
    summary: str
    homepage: str | None
    repository: str | None
    author: str | None


def _load_package_info() -> PackageInfo:
    try:
        distribution_metadata = metadata(DIST_NAME)
        package_version = version(DIST_NAME)
    except PackageNotFoundError:
        return PackageInfo(
            name="S3 Object Browser",
            version="",
            summary="Browse buckets and objects stored in Amazon S3.\nCreated with Tkinter.",
            homepage=None,
            repository=None,
            author=None,
        )
    summary = distribution_metadata.get("Summary") or ""
    author = distribution_metadata.get("Author") or distribution_metadata.get("Author-email")
    homepage = distribution_metadata.get("Home-page")
    repository = None
    for entry in distribution_metadata.get_all("Project-URL") or []:
        label, _, link = entry.partition(",")
        label = label.strip().lower()
        url = link.strip()
        if label == "repository":
            repository = url
        elif label == "homepage" and not homepage:
            homepage = url
    return PackageInfo(
        name=distribution_metadata.get("Name"),
        version=package_version,
        summary=summary,
        homepage=homepage or None,
        repository=repository,
        author=author or None,
    )


def _split_size_bytes(size_bytes: int) -> tuple[str, str]:
    if size_bytes <= 0:
        return ("1", "MB")
    for unit in ("MB", "KB"):
        factor = SIZE_UNIT_FACTORS[unit]
        if size_bytes >= factor and size_bytes % factor == 0:
            return (str(size_bytes // factor), unit)
    return (str(size_bytes), "B")


def _parse_size_bytes(value: str, unit: str) -> int | None:
    try:
        amount = int(value.strip())
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None
    factor = SIZE_UNIT_FACTORS.get(unit.strip().upper())
    if not factor:
        return None
    return amount * factor


class S3BrowserApp:
    """Tkinter view that delegates business logic to :class:`S3BrowserController`."""

    def __init__(self, root: tk.Tk, controller: S3BrowserController | None = None):
        self.root = root
        self.root.title("S3 Object Browser")
        self.root.geometry("800x980")
        self.root.minsize(600, 480)

        self.controller = controller or S3BrowserController()
        self._operation_in_progress = False
        self._node_state: dict[str, dict[str, object]] = {}
        self._settings_storage = SettingsStorage()
        self._app_settings: AppSettings = self._settings_storage.load()
        initial_fetch_limit = self._app_settings.fetch_limit if self._app_settings.fetch_limit > 0 else 10
        self.max_keys_var = tk.StringVar(value=str(initial_fetch_limit))
        self._update_fetch_limit(initial_fetch_limit, persist=False, trigger_refresh=False)
        self._pending_object_refresh = False
        self._objects_menu: tk.Menu | None = None
        self._file_menu: tk.Menu | None = None
        self._connection_menu: tk.Menu | None = None
        self._bucket_menu: tk.Menu | None = None
        self._settings_window: tk.Toplevel | None = None
        self._about_window: tk.Toplevel | None = None
        self._upload_menu_label = "Upload..."
        self._download_menu_label = "Download File..."
        self._signed_url_menu_label = "Generate Signed URL..."
        self._bucket_refresh_label = "Refresh Buckets"
        self._bucket_names: list[str] = []
        self._transfer_dialog: TransferDialog | None = None
        self._object_context_menu: tk.Menu | None = None
        self._folder_context_menu: tk.Menu | None = None
        self._active_context_menu: tk.Menu | None = None
        self.connection_var = tk.StringVar()
        self._package_info = _load_package_info()

        self._create_menu()
        self._create_widgets()
        self._create_context_menus()
        self._refresh_selection_controls()
        self.root.bind("<Button-1>", self._handle_left_click, add="+")
        self._auto_connect_if_enabled()

    def create_connection(self, *, connect_on_save: bool = False) -> None:
        primary_action = "save_and_connect" if connect_on_save else "save"
        primary_label = "Save and Connect" if connect_on_save else "Save"
        dialog = ConnectionDialog(
            self.root,
            title="Create Connection",
            primary_action=primary_action,
            primary_label=primary_label,
        )
        result = dialog.show()
        self._apply_connection_dialog_result(result)

    def edit_connection(self, profile_name: str | None = None, *, connect_on_save: bool = False) -> None:
        target_name = profile_name or self.connection_var.get()
        if not target_name:
            return
        try:
            profile = self.controller.get_profile(target_name)
        except ValueError as exc:
            messagebox.showerror("Error", str(exc))
            return
        primary_action = "save_and_connect" if connect_on_save else "save"
        primary_label = "Save and Connect" if connect_on_save else "Save"
        dialog = ConnectionDialog(
            self.root,
            title="Edit Connection",
            profile=profile,
            primary_action=primary_action,
            primary_label=primary_label,
        )
        result = dialog.show()
        self._apply_connection_dialog_result(result)

    def _apply_connection_dialog_result(self, result: dict | None) -> None:
        if not result:
            return
        action = result["action"]
        if action == "connect":
            self.connect(result["name"])
        elif action in {"save", "save_and_connect"}:
            profile: ConnectionProfile = result["profile"]
            try:
                self.controller.save_profile(profile, original_name=result.get("original_name"))
            except ValueError as exc:
                messagebox.showerror("Error", str(exc))
                return
            self._refresh_connection_menu(selected_name=profile.name)
            if action == "save_and_connect":
                self.connect(profile.name)
        elif action == "delete":
            try:
                self.controller.delete_profile(result["name"])
            except ValueError as exc:
                messagebox.showerror("Error", str(exc))
                return
            if self._app_settings.remember_last_bucket and result["name"] == self._app_settings.last_connection:
                self._app_settings.last_connection = ""
                self._settings_storage.save(self._app_settings)
            self._refresh_connection_menu()

    def _refresh_connection_menu(self, selected_name: str | None = None) -> None:
        profiles = self.controller.list_profiles()
        names = [profile.name for profile in profiles]
        current = self.connection_var.get()
        if selected_name and selected_name in names:
            self.connection_var.set(selected_name)
        elif current in names:
            pass
        elif names:
            preferred = ""
            if self._app_settings.remember_last_bucket:
                candidate = self._app_settings.last_connection
                if candidate in names:
                    preferred = candidate
            self.connection_var.set(preferred or names[0])
        else:
            self.connection_var.set("")
        if self._connection_menu:
            self._connection_menu.delete(0, tk.END)
            self._connection_menu.add_command(label="Create New Connection", command=self.create_connection)
            self._connection_menu.add_separator()
            if names:
                for name in names:
                    self._connection_menu.add_command(
                        label=name,
                        command=lambda value=name: self._open_connection_from_menu(value),
                    )
            else:
                self._connection_menu.add_command(label="No saved connections", state="disabled")
        self._refresh_upload_controls()
        self._refresh_signed_url_controls()

    def _open_connection_from_menu(self, profile_name: str) -> None:
        self.edit_connection(profile_name=profile_name, connect_on_save=True)

    def _create_menu(self) -> None:
        menubar = tk.Menu(self.root)

        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(
            label=self._upload_menu_label,
            command=self.upload_file,
            state="disabled",
        )
        file_menu.add_command(
            label=self._download_menu_label,
            command=self._download_selected_object,
            state="disabled",
        )
        file_menu.add_command(
            label=self._signed_url_menu_label,
            command=self.open_signed_url_dialog,
            state="disabled",
        )
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.destroy)
        menubar.add_cascade(label="File", menu=file_menu)
        self._file_menu = file_menu

        connection_menu = tk.Menu(menubar, tearoff=0)
        self._connection_menu = connection_menu
        menubar.add_cascade(label="Connection", menu=connection_menu)

        bucket_menu = tk.Menu(menubar, tearoff=0)
        self._bucket_menu = bucket_menu
        menubar.add_cascade(label="Buckets", menu=bucket_menu)

        objects_menu = tk.Menu(menubar, tearoff=0)
        objects_menu.add_command(label="Refresh", command=self.list_objects, state="disabled")
        menubar.add_cascade(label="Objects", menu=objects_menu)
        self._objects_menu = objects_menu

        options_menu = tk.Menu(menubar, tearoff=0)
        options_menu.add_command(label="Settings", command=self.open_settings_dialog)
        menubar.add_cascade(label="Options", menu=options_menu)

        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about_dialog)
        menubar.add_cascade(label="Help", menu=help_menu)

        self.root.config(menu=menubar)
        self._refresh_connection_menu()
        self._render_bucket_menu()

    def _create_widgets(self) -> None:
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(1, weight=1)

        bucket_frame = ttk.Frame(main_frame)
        bucket_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))
        bucket_frame.columnconfigure(1, weight=1)

        ttk.Label(bucket_frame, text="Bucket:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.bucket_var = tk.StringVar()
        self.bucket_label_var = tk.StringVar(value="No bucket selected")
        self.bucket_var.trace_add("write", lambda *_: self._update_bucket_label())
        ttk.Label(bucket_frame, textvariable=self.bucket_label_var).grid(
            row=0, column=1, sticky=(tk.W, tk.E), pady=2, padx=(5, 5)
        )

        self.upload_button = ttk.Button(
            bucket_frame,
            text="Upload File",
            command=self.upload_file,
            state="disabled",
        )
        self.upload_button.grid(row=0, column=2, pady=2, padx=(5, 0))

        tree_frame = ttk.Frame(main_frame)
        tree_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(20, 0))
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        self.results_tree = ttk.Treeview(tree_frame, show="tree", selectmode="browse")
        self.results_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        tree_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=self.results_tree.yview)
        tree_scroll_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        tree_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.results_tree.xview)
        tree_scroll_x.grid(row=1, column=0, sticky=(tk.W, tk.E))
        self.results_tree.configure(yscrollcommand=tree_scroll_y.set, xscrollcommand=tree_scroll_x.set)
        self.results_tree.bind("<<TreeviewOpen>>", self._handle_tree_open)
        self.results_tree.bind("<Double-1>", self._handle_tree_double_click)
        self.results_tree.bind("<<TreeviewSelect>>", lambda _: self._refresh_selection_controls())
        self.results_tree.bind("<Button-3>", self._handle_tree_right_click)
        self.results_tree.bind("<Button-2>", self._handle_tree_right_click)

        self.progress = ttk.Progressbar(main_frame, mode="indeterminate")
        self.progress.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=5)

        self.status_var = tk.StringVar(value="Ready")
        self.status_label = ttk.Label(main_frame, textvariable=self.status_var, anchor=tk.W)
        self.status_label.grid(row=3, column=0, sticky=(tk.W, tk.E))

    def _create_context_menus(self) -> None:
        object_menu = tk.Menu(self.root, tearoff=0)
        object_menu.add_command(label="Info", command=self._open_selected_object_info)
        object_menu.add_command(label="Download", command=self._download_selected_object)
        object_menu.add_command(label="Get Signed URL", command=self._open_signed_url_for_selection)
        object_menu.add_separator()
        object_menu.add_command(label="Delete", command=self._delete_selected_object)
        object_menu.bind("<Unmap>", lambda _event, menu=object_menu: self._on_context_menu_unmap(menu))
        self._object_context_menu = object_menu

        folder_menu = tk.Menu(self.root, tearoff=0)
        folder_menu.add_command(label=self._upload_menu_label, command=self.upload_file)
        folder_menu.add_command(label="Refresh", command=self._refresh_selected_folder)
        folder_menu.add_command(label="Get Signed URL", command=self._open_signed_url_for_selection)
        folder_menu.bind("<Unmap>", lambda _event, menu=folder_menu: self._on_context_menu_unmap(menu))
        self._folder_context_menu = folder_menu

    def _handle_left_click(self, _event) -> None:
        self._dismiss_context_menu()

    def _dismiss_context_menu(self) -> None:
        if not self._active_context_menu:
            return
        try:
            self._active_context_menu.unpost()
        except tk.TclError:
            pass
        self._active_context_menu = None

    def _on_context_menu_unmap(self, menu: tk.Menu) -> None:
        if self._active_context_menu is menu:
            self._active_context_menu = None

    def show_about_dialog(self) -> None:
        if self._about_window and self._about_window.winfo_exists():
            self._about_window.lift()
            return

        window = tk.Toplevel(self.root)
        window.title("About")
        window.resizable(False, False)
        window.transient(self.root)
        window.grab_set()

        frame = ttk.Frame(window, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        package_heading = self._package_info.name
        if self._package_info.version:
            package_heading = f"{package_heading} v{self._package_info.version}"
        ttk.Label(frame, text=package_heading, font=("TkDefaultFont", 14, "bold")).pack(pady=(0, 5))
        if self._package_info.summary:
            ttk.Label(
                frame,
                text=self._package_info.summary,
                justify="center",
                wraplength=360,
            ).pack(pady=(0, 10))
        if self._package_info.author:
            ttk.Label(frame, text=f"Author: {self._package_info.author}", justify="center").pack(pady=(0, 5))

        info_frame = ttk.Frame(frame)
        info_frame.pack(fill=tk.X, pady=(0, 10))
        if self._package_info.homepage:
            ttk.Label(info_frame, text=f"Homepage: {self._package_info.homepage}", justify="center").pack(
                anchor=tk.CENTER
            )
        if self._package_info.repository:
            ttk.Label(info_frame, text=f"Repository: {self._package_info.repository}", justify="center").pack(
                anchor=tk.CENTER
            )

        ttk.Button(frame, text="Close", command=self._close_about_window).pack()

        self._about_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_about_window)

    def _close_about_window(self) -> None:
        if self._about_window and self._about_window.winfo_exists():
            self._about_window.destroy()
        self._about_window = None

    def open_settings_dialog(self) -> None:
        if self._settings_window and self._settings_window.winfo_exists():
            self._settings_window.lift()
            return

        window = tk.Toplevel(self.root)
        window.title("Settings")
        window.resizable(False, False)
        window.transient(self.root)
        window.grab_set()

        frame = ttk.Frame(window, padding=20)
        frame.pack(fill=tk.BOTH, expand=True)

        temp_var = tk.StringVar(value=self.max_keys_var.get())
        default_size_value, default_size_unit = _split_size_bytes(self._app_settings.default_post_max_size)
        size_var = tk.StringVar(value=default_size_value)
        size_unit_var = tk.StringVar(value=default_size_unit)
        remember_bucket_var = tk.BooleanVar(value=self._app_settings.remember_last_bucket)

        ttk.Label(frame, text="Fetch limit:").grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        entry = ttk.Entry(frame, textvariable=temp_var, width=10, justify="right")
        entry.grid(row=0, column=1, sticky=tk.W, pady=(0, 10), padx=(5, 0))

        ttk.Label(frame, text="Default POST max size:").grid(row=1, column=0, sticky=tk.W, pady=(0, 10))
        size_frame = ttk.Frame(frame)
        size_frame.grid(row=1, column=1, sticky=tk.W, pady=(0, 10), padx=(5, 0))
        size_entry = ttk.Entry(size_frame, textvariable=size_var, width=10, justify="right")
        size_entry.grid(row=0, column=0, sticky=tk.W)
        size_unit = ttk.Combobox(
            size_frame,
            textvariable=size_unit_var,
            values=SIZE_UNITS,
            state="readonly",
            width=6,
        )
        size_unit.grid(row=0, column=1, sticky=tk.W, padx=(6, 0))

        remember_checkbox = ttk.Checkbutton(
            frame,
            text="Remember last active connection & bucket",
            variable=remember_bucket_var,
            onvalue=True,
            offvalue=False,
        )
        remember_checkbox.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(0, 10))

        buttons = ttk.Frame(frame)
        buttons.grid(row=3, column=0, columnspan=2, pady=(5, 0), sticky=tk.E)

        def save_settings() -> None:
            try:
                max_keys = int(temp_var.get().strip())
            except ValueError:
                messagebox.showerror("Error", "Max objects must be a whole number")
                return
            if max_keys <= 0:
                messagebox.showerror("Error", "Max objects must be greater than zero")
                return
            max_size = _parse_size_bytes(size_var.get(), size_unit_var.get())
            if max_size is None:
                messagebox.showerror("Error", "Default POST max size must be a whole number greater than zero")
                return
            self._update_fetch_limit(max_keys, trigger_refresh=False)
            self._app_settings.default_post_max_size = max_size
            self._app_settings.remember_last_bucket = bool(remember_bucket_var.get())
            if self._app_settings.remember_last_bucket:
                current_bucket = self.bucket_var.get().strip()
                if current_bucket:
                    self._app_settings.last_bucket = current_bucket
                current_connection = self.connection_var.get().strip()
                if current_connection:
                    self._app_settings.last_connection = current_connection
            self._settings_storage.save(self._app_settings)
            self._close_settings_window()

        ttk.Button(buttons, text="Save", command=save_settings).grid(row=0, column=0, padx=(0, 5))
        ttk.Button(buttons, text="Cancel", command=self._close_settings_window).grid(row=0, column=1)

        entry.focus()
        self._settings_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_settings_window)

    def _close_settings_window(self) -> None:
        if self._settings_window and self._settings_window.winfo_exists():
            self._settings_window.destroy()
        self._settings_window = None

    def connect(self, profile_name: str | None = None) -> None:
        target_name = profile_name or self.connection_var.get()
        if not target_name:
            messagebox.showerror("Error", "Please choose a connection from the Connection menu")
            return
        self.connection_var.set(target_name)

        self._start_operation()

        thread = threading.Thread(
            target=self._connect_thread,
            args=(target_name,),
            daemon=True,
        )
        thread.start()

    def refresh_buckets(self) -> None:
        if not self.controller.is_connected:
            messagebox.showerror("Error", "Please connect first")
            return

        self._start_operation()
        thread = threading.Thread(
            target=self._refresh_buckets_thread,
            daemon=True,
        )
        thread.start()

    def list_objects(self) -> None:
        self._pending_object_refresh = False
        if not self.controller.is_connected:
            messagebox.showerror("Error", "Please connect first")
            return

        bucket_name = self.bucket_var.get()
        if not bucket_name:
            messagebox.showerror("Error", "Please select a bucket")
            return

        try:
            max_keys = int(self.max_keys_var.get().strip())
        except ValueError:
            messagebox.showerror("Error", "Max objects must be a whole number")
            return

        if max_keys <= 0:
            messagebox.showerror("Error", "Max objects must be greater than zero")
            return

        self._current_max_keys = max_keys
        self._clear_tree()
        self._start_operation()

        thread = threading.Thread(
            target=self._list_objects_thread,
            args=(bucket_name, max_keys),
            daemon=True,
        )
        thread.start()

    def _connect_thread(self, profile_name: str) -> None:
        try:
            buckets = self.controller.connect_with_profile(profile_name)
            self.root.after(0, lambda: self._handle_connect_success(buckets))
        except ValueError as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: messagebox.showerror("Error", msg))
        except (ClientError, BotoCoreError) as exc:
            self._show_error("Connection Error", f"Error connecting to S3: {exc}")
        except Exception as exc:  # pragma: no cover - defensive
            self._show_error("Connection Error", f"Unexpected error: {exc}")
        finally:
            self.root.after(0, self._end_operation)

    def _refresh_buckets_thread(self) -> None:
        try:
            buckets = self.controller.refresh_buckets()
            self.root.after(0, lambda: self._update_bucket_menu(buckets))
        except (ClientError, BotoCoreError) as exc:
            self._show_error("Bucket Error", f"Error refreshing buckets: {exc}")
        except Exception as exc:  # pragma: no cover - defensive
            self._show_error("Bucket Error", f"Unexpected error: {exc}")
        finally:
            self.root.after(0, self._end_operation)

    def _list_objects_thread(self, bucket_name: str, max_keys: int) -> None:
        try:
            listing = self.controller.list_objects(bucket_name=bucket_name, max_keys=max_keys)
            self._display_results([listing], on_complete=self._end_operation)
        except (ClientError, BotoCoreError) as exc:
            self._show_error("List Error", f"Error listing objects: {exc}")
            self.root.after(0, self._end_operation)
        except Exception as exc:  # pragma: no cover - defensive
            self._show_error("List Error", f"Unexpected error: {exc}")
            self.root.after(0, self._end_operation)

    def _handle_connect_success(self, buckets: list[str]) -> None:
        if self._app_settings.remember_last_bucket:
            selected = self.controller.selected_profile or self.connection_var.get().strip()
            if selected:
                self._app_settings.last_connection = selected
                self._settings_storage.save(self._app_settings)
        self._update_bucket_menu(buckets)
        self._refresh_connection_menu()
        self._set_status("Connected. Buckets loaded.")

    def _auto_connect_if_enabled(self) -> None:
        if not self._app_settings.remember_last_bucket:
            return
        last_connection = self._app_settings.last_connection
        if not last_connection:
            return
        names = [profile.name for profile in self.controller.list_profiles()]
        if last_connection not in names:
            return
        self.connection_var.set(last_connection)
        self.root.after(0, lambda: self.connect(last_connection))

    def _update_bucket_menu(self, buckets: list[str]) -> None:
        self._bucket_names = list(buckets)
        current = self.bucket_var.get()
        if current not in self._bucket_names:
            preferred = ""
            if self._app_settings.remember_last_bucket:
                candidate = self._app_settings.last_bucket
                if candidate in self._bucket_names:
                    preferred = candidate
            new_value = preferred or (self._bucket_names[0] if self._bucket_names else "")
            if new_value != current:
                self.bucket_var.set(new_value)
                if new_value:
                    self._on_bucket_selected()
            else:
                self.bucket_var.set(new_value)
        else:
            if current:
                self._schedule_object_refresh()
        self._render_bucket_menu()
        self._refresh_upload_controls()

    def _render_bucket_menu(self) -> None:
        if not self._bucket_menu:
            return
        self._bucket_menu.delete(0, tk.END)
        refresh_state = "normal" if self.controller.is_connected and not self._operation_in_progress else "disabled"
        self._bucket_menu.add_command(
            label=self._bucket_refresh_label,
            command=self.refresh_buckets,
            state=refresh_state,
        )
        self._bucket_menu.add_separator()
        if self._bucket_names:
            entry_state = "normal" if (self.controller.is_connected and not self._operation_in_progress) else "disabled"
            current = self.bucket_var.get()
            for name in self._bucket_names:
                label = name if name != current else f"{name} (current)"
                self._bucket_menu.add_command(
                    label=label,
                    command=lambda value=name: self._select_bucket_from_menu(value),
                    state=entry_state,
                )
        else:
            if self.controller.is_connected:
                placeholder = "No buckets loaded"
            else:
                placeholder = "Connect to load buckets"
            self._bucket_menu.add_command(label=placeholder, state="disabled")

    def _select_bucket_from_menu(self, bucket_name: str) -> None:
        if not self.controller.is_connected or self._operation_in_progress:
            return
        if bucket_name not in self._bucket_names:
            return
        current = self.bucket_var.get()
        if bucket_name == current:
            self._schedule_object_refresh()
            return
        self.bucket_var.set(bucket_name)
        self._on_bucket_selected()

    def _start_operation(self) -> None:
        self._operation_in_progress = True
        self._set_objects_menu_state("disabled")
        self._set_upload_controls_state("disabled")
        self._refresh_selection_controls()
        self._render_bucket_menu()
        self.progress.start()

    def _end_operation(self) -> None:
        self._operation_in_progress = False
        self.progress.stop()
        is_connected = self.controller.is_connected
        refresh_state = "normal" if is_connected else "disabled"
        self._set_objects_menu_state(refresh_state)
        self._refresh_upload_controls()
        self._refresh_selection_controls()
        self._render_bucket_menu()
        self._perform_pending_object_refresh()

    def _on_bucket_selected(self) -> None:
        self._refresh_upload_controls()
        if self._app_settings.remember_last_bucket:
            bucket_name = self.bucket_var.get().strip()
            if bucket_name and bucket_name != self._app_settings.last_bucket:
                self._app_settings.last_bucket = bucket_name
                self._settings_storage.save(self._app_settings)
        self._schedule_object_refresh()

    def _schedule_object_refresh(self) -> None:
        if not self.controller.is_connected:
            return
        if not self.bucket_var.get():
            return
        if self._operation_in_progress:
            self._pending_object_refresh = True
            return
        self.list_objects()

    def _perform_pending_object_refresh(self) -> None:
        if not self._pending_object_refresh:
            return
        self._pending_object_refresh = False
        if not self.controller.is_connected:
            return
        if not self.bucket_var.get():
            return
        self.list_objects()

    def _set_objects_menu_state(self, state: str) -> None:
        if self._objects_menu:
            self._objects_menu.entryconfig("Refresh", state=state)

    def _update_fetch_limit(self, value: int, *, persist: bool = True, trigger_refresh: bool = True) -> None:
        try:
            sanitized = int(value)
        except (TypeError, ValueError):
            sanitized = self._current_max_keys or 10
        if sanitized <= 0:
            sanitized = 1
        self._current_max_keys = sanitized
        self.max_keys_var.set(str(sanitized))
        self._app_settings.fetch_limit = sanitized
        if persist:
            self._settings_storage.save(self._app_settings)
        if trigger_refresh:
            self._schedule_object_refresh()

    def _update_bucket_label(self) -> None:
        if hasattr(self, "bucket_label_var"):
            value = self.bucket_var.get().strip()
            display = value or "No bucket selected"
            self.bucket_label_var.set(display)

    def _refresh_upload_controls(self) -> None:
        if self._operation_in_progress:
            self._set_upload_controls_state("disabled")
            return
        enabled = self.controller.is_connected and bool(self.bucket_var.get())
        state = "normal" if enabled else "disabled"
        self._set_upload_controls_state(state)

    def _set_upload_controls_state(self, state: str) -> None:
        if hasattr(self, "upload_button"):
            self.upload_button.config(state=state)
        if self._file_menu:
            self._file_menu.entryconfig(self._upload_menu_label, state=state)
        self._refresh_signed_url_controls()

    def _refresh_signed_url_controls(self) -> None:
        state = "normal" if self.controller.is_connected else "disabled"
        if self._file_menu:
            self._file_menu.entryconfig(self._signed_url_menu_label, state=state)

    def _refresh_selected_folder(self) -> None:
        if not self.controller.is_connected:
            messagebox.showerror("Error", "Please connect first")
            return
        selected = self._get_selected_node()
        if not selected:
            return
        node_id, node_info = selected
        node_type = node_info.get("type")
        if node_type not in {"bucket", "prefix"}:
            return
        if node_info.get("loading"):
            return
        bucket = node_info.get("bucket")
        if not bucket:
            return
        prefix = node_info.get("prefix", "") if node_type == "prefix" else ""
        node_info["loading"] = True
        thread = threading.Thread(
            target=self._refresh_folder_thread,
            args=(node_id, bucket, prefix),
            daemon=True,
        )
        thread.start()

    def _refresh_folder_thread(self, node_id: str, bucket: str, prefix: str) -> None:
        try:
            listing = self.controller.list_objects(
                bucket_name=bucket,
                max_keys=self._current_max_keys,
                prefix=prefix,
            )
            self.root.after(0, lambda: self._render_prefix_listing(node_id, listing))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_prefix_error(node_id, msg))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_prefix_error(node_id, msg))

    def _get_selected_node(self) -> tuple[str, dict[str, object]] | None:
        selection = self.results_tree.focus()
        if not selection:
            selected = self.results_tree.selection()
            if selected:
                selection = selected[0]
        if not selection:
            return None
        node_info = self._node_state.get(selection)
        if not node_info:
            return None
        return selection, node_info

    def _get_selected_upload_target(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            return None
        _, node_info = selected
        node_type = node_info.get("type")
        bucket = node_info.get("bucket") or self.bucket_var.get()
        if not bucket:
            return None
        prefix = ""
        if node_type in {"bucket", "prefix"}:
            prefix = node_info.get("prefix", "") or ""
        elif node_type == "object":
            key = node_info.get("key", "") or ""
            prefix = key.rsplit("/", 1)[0] + "/" if "/" in key else ""
        elif node_type == "load_more":
            prefix = node_info.get("prefix", "") or ""
        return bucket, prefix

    def _get_selected_object_path(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            return None
        _, node_info = selected
        node_type = node_info.get("type")
        bucket = node_info.get("bucket") or self.bucket_var.get()
        if not bucket:
            return None
        if node_type == "object":
            return bucket, node_info.get("key", "") or ""
        if node_type in {"prefix", "load_more"}:
            return bucket, node_info.get("prefix", "") or ""
        if node_type == "bucket":
            return bucket, ""
        return None

    def _get_selected_object(self) -> tuple[str, str] | None:
        selected = self._get_selected_node()
        if not selected:
            return None
        _, node_info = selected
        if node_info.get("type") != "object":
            return None
        bucket = node_info.get("bucket") or self.bucket_var.get()
        key = node_info.get("key")
        if not bucket or not key:
            return None
        return bucket, key

    def _open_selected_object_info(self) -> None:
        target = self._get_selected_object()
        if not target:
            messagebox.showerror("Info", "Please select a file to inspect.")
            return
        bucket, key = target
        self._show_object_details(bucket, key)

    def _download_selected_object(self) -> None:
        target = self._get_selected_object()
        if not target:
            messagebox.showerror("Download", "Please select a file to download.")
            return
        bucket, key = target
        self._download_object(bucket, key)

    def _delete_selected_object(self) -> None:
        target = self._get_selected_object()
        if not target:
            messagebox.showerror("Delete", "Please select a file to delete.")
            return
        bucket, key = target
        confirmed = messagebox.askyesno(
            "Delete Object",
            f"Delete '{key}' from bucket '{bucket}'?",
            parent=self.root,
        )
        if not confirmed:
            return
        self._delete_object(bucket, key)

    def _open_signed_url_for_selection(self) -> None:
        selection = self._get_selected_object_path()
        bucket_name = selection[0] if selection else self.bucket_var.get()
        key_name = selection[1] if selection else ""
        if not bucket_name:
            messagebox.showerror("Signed URL", "Please select a bucket first")
            return
        self.open_signed_url_dialog(bucket=bucket_name, key=key_name)

    def _has_object_selection(self) -> bool:
        return self._get_selected_object() is not None

    def _refresh_selection_controls(self) -> None:
        if self._operation_in_progress or not self.controller.is_connected:
            state = "disabled"
        else:
            state = "normal" if self._has_object_selection() else "disabled"
        self._set_download_controls_state(state)

    def _set_download_controls_state(self, state: str) -> None:
        if self._file_menu:
            self._file_menu.entryconfig(self._download_menu_label, state=state)

    def _compose_s3_key(self, prefix: str, name: str) -> str:
        key_name = name.strip()
        if not key_name:
            raise ValueError("Object name cannot be empty")
        cleaned_prefix = prefix.strip()
        cleaned_prefix = cleaned_prefix.lstrip("/")
        if cleaned_prefix and not cleaned_prefix.endswith("/"):
            cleaned_prefix += "/"
        return f"{cleaned_prefix}{key_name}" if cleaned_prefix else key_name

    def _display_results(
        self,
        bucket_listings: list[BucketListing],
        *,
        on_complete: Callable[[], None] | None = None,
    ) -> None:
        def _render() -> None:
            try:
                self._populate_tree(bucket_listings)
            finally:
                if on_complete:
                    on_complete()

        self.root.after(0, _render)

    def _populate_tree(self, bucket_listings: list[BucketListing]) -> None:
        self._clear_tree()
        total_objects = 0
        total_prefixes = 0

        for bucket in bucket_listings:
            bucket_id = self.results_tree.insert("", "end", text=bucket.name, open=True)
            self._node_state[bucket_id] = {
                "type": "bucket",
                "bucket": bucket.name,
                "prefix": bucket.prefix or "",
            }
            if bucket.error:
                self.results_tree.insert(bucket_id, "end", text=f"Error: {bucket.error}")
                continue

            objects_added, prefixes_added = self._render_listing_contents(bucket_id, bucket)
            total_objects += objects_added
            total_prefixes += prefixes_added
            if not (objects_added or prefixes_added):
                self.results_tree.insert(bucket_id, "end", text="(No objects)")

        if total_objects or total_prefixes:
            self._set_status(f"Loaded {total_objects} object(s) and {total_prefixes} folder(s).")
        else:
            self._set_status("No objects found.")
        self._refresh_selection_controls()

    def _render_listing_contents(
        self,
        parent_id: str,
        listing: BucketListing,
    ) -> tuple[int, int]:
        objects_added = 0
        prefixes_added = 0
        for page in listing.pages:
            if page.error:
                self.results_tree.insert(
                    parent_id,
                    "end",
                    text=f"Page {page.number} error: {page.error}",
                )
                continue
            for prefix in page.prefixes:
                self._insert_prefix_node(parent_id, listing.name, prefix, listing.prefix)
                prefixes_added += 1
            for key in page.keys:
                self._insert_file_node(parent_id, listing.name, key, listing.prefix)
                objects_added += 1
        self._refresh_load_more_node(parent_id, listing)
        return objects_added, prefixes_added

    def _insert_prefix_node(self, parent_id: str, bucket: str, prefix: str, base_prefix: str) -> str:
        label = self._relative_name(prefix, base_prefix)
        node_id = self.results_tree.insert(parent_id, "end", text=label, open=False)
        self.results_tree.insert(node_id, "end", text="Loading...")
        self._node_state[node_id] = {
            "type": "prefix",
            "bucket": bucket,
            "prefix": prefix,
            "loaded": False,
            "loading": False,
        }
        return node_id

    def _insert_file_node(self, parent_id: str, bucket: str, key: str, base_prefix: str) -> None:
        label = self._relative_name(key, base_prefix)
        node_id = self.results_tree.insert(parent_id, "end", text=label)
        self._node_state[node_id] = {"type": "object", "bucket": bucket, "key": key}

    def _find_node(self, *, node_type: str, bucket: str, key: str | None = None, prefix: str | None = None) -> str | None:
        for node_id, info in self._node_state.items():
            if info.get("type") != node_type or info.get("bucket") != bucket:
                continue
            if key is not None and info.get("key") != key:
                continue
            if prefix is not None and info.get("prefix") != prefix:
                continue
            if self.results_tree.exists(node_id):
                return node_id
        return None

    def _node_has_content(self, node_id: str) -> bool:
        for child in self.results_tree.get_children(node_id):
            if child in self._node_state:
                return True
        return False

    def _remove_placeholder_children(self, parent_id: str) -> None:
        placeholders = {"(No objects)", "(Empty)"}
        for child in list(self.results_tree.get_children(parent_id)):
            if child in self._node_state:
                continue
            text = self.results_tree.item(child, "text")
            if text in placeholders:
                self.results_tree.delete(child)

    def _prune_empty_parents(self, node_id: str) -> None:
        current = node_id
        while current:
            node_info = self._node_state.get(current)
            if not node_info:
                return
            node_type = node_info.get("type")
            if node_type == "bucket":
                if self._node_has_content(current):
                    return
                self._remove_placeholder_children(current)
                self.results_tree.insert(current, "end", text="(No objects)")
                return
            if node_type != "prefix":
                return
            if self._node_has_content(current):
                return
            parent_id = self.results_tree.parent(current)
            self._delete_subtree(current)
            current = parent_id

    def _remove_object_from_tree(self, bucket: str, key: str) -> bool:
        node_id = self._find_node(node_type="object", bucket=bucket, key=key)
        if not node_id:
            return False
        parent_id = self.results_tree.parent(node_id)
        self._delete_subtree(node_id)
        if parent_id:
            self._prune_empty_parents(parent_id)
        self._refresh_selection_controls()
        return True

    def _ensure_prefix_chain(self, bucket_id: str, bucket: str, prefix: str) -> tuple[str | None, bool]:
        segments = [segment for segment in prefix.strip("/").split("/") if segment]
        current_parent = bucket_id
        current_prefix = ""
        created = False
        for segment in segments:
            current_prefix = f"{current_prefix}{segment}/"
            existing = self._find_node(node_type="prefix", bucket=bucket, prefix=current_prefix)
            if existing:
                current_parent = existing
                continue
            parent_info = self._node_state.get(current_parent, {})
            if parent_info.get("type") == "prefix" and not parent_info.get("loaded"):
                return None, True
            base_prefix = parent_info.get("prefix", "")
            current_parent = self._insert_prefix_node(current_parent, bucket, current_prefix, base_prefix)
            created = True
        return current_parent, created

    def _add_object_to_tree(self, bucket: str, key: str) -> bool:
        if bucket != self.bucket_var.get():
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
        if prefix:
            prefix_id, created = self._ensure_prefix_chain(bucket_id, bucket, prefix)
            if not prefix_id:
                return False
            prefix_info = self._node_state.get(prefix_id, {})
            if not prefix_info.get("loaded") or created:
                return True
            parent_id = prefix_id
        base_prefix = ""
        parent_info = self._node_state.get(parent_id, {})
        if parent_info.get("type") == "prefix":
            base_prefix = parent_info.get("prefix", "")
        self._remove_placeholder_children(parent_id)
        self._insert_file_node(parent_id, bucket, key, base_prefix)
        self._refresh_selection_controls()
        return True

    def _refresh_load_more_node(self, parent_id: str, listing: BucketListing) -> None:
        self._remove_load_more_nodes(parent_id)
        if listing.has_more and listing.continuation_token:
            self._insert_load_more_node(parent_id, listing)

    def _remove_load_more_nodes(self, parent_id: str) -> None:
        for child in list(self.results_tree.get_children(parent_id)):
            node_info = self._node_state.get(child)
            if node_info and node_info.get("type") == "load_more":
                self._delete_subtree(child)

    def _insert_load_more_node(self, parent_id: str, listing: BucketListing) -> None:
        node_id = self.results_tree.insert(parent_id, "end", text="Load more...")
        delimiter = listing.delimiter or None
        self._node_state[node_id] = {
            "type": "load_more",
            "bucket": listing.name,
            "prefix": listing.prefix,
            "delimiter": delimiter,
            "continuation_token": listing.continuation_token,
            "parent": parent_id,
            "loading": False,
        }

    def _relative_name(self, value: str, base_prefix: str) -> str:
        relative = value
        if base_prefix and value.startswith(base_prefix):
            relative = value[len(base_prefix) :]
        relative = relative.rstrip("/")
        if not relative:
            trimmed = value.rstrip("/")
            relative = trimmed or value
        return relative

    def _handle_tree_open(self, event) -> None:
        tree = event.widget
        item_id = tree.focus()
        if not item_id:
            return
        node_info = self._node_state.get(item_id)
        if not node_info or node_info.get("type") != "prefix":
            return
        if node_info.get("loaded") or node_info.get("loading"):
            return
        node_info["loading"] = True
        thread = threading.Thread(
            target=self._load_prefix_thread,
            args=(item_id, node_info["bucket"], node_info["prefix"]),
            daemon=True,
        )
        thread.start()

    def _handle_tree_double_click(self, event) -> None:
        item_id = self.results_tree.focus()
        if not item_id:
            return
        node_info = self._node_state.get(item_id)
        if not node_info:
            return
        node_type = node_info.get("type")
        if node_type == "load_more":
            if node_info.get("loading") or not node_info.get("continuation_token"):
                return
            node_info["loading"] = True
            self.results_tree.item(item_id, text="Loading more...")
            thread = threading.Thread(
                target=self._load_more_thread,
                args=(
                    item_id,
                    node_info["parent"],
                    node_info["bucket"],
                    node_info.get("prefix", ""),
                    node_info.get("delimiter"),
                    node_info["continuation_token"],
                ),
                daemon=True,
            )
            thread.start()
        elif node_type == "object":
            self._show_object_details(node_info["bucket"], node_info["key"])

    def _handle_tree_right_click(self, event) -> str | None:
        item_id = self.results_tree.identify_row(event.y)
        if not item_id:
            return None
        node_info = self._node_state.get(item_id)
        if not node_info:
            return "break"
        self.results_tree.selection_set(item_id)
        self.results_tree.focus(item_id)
        self._refresh_selection_controls()
        node_type = node_info.get("type")
        menu: tk.Menu | None = None
        if node_type == "object":
            menu = self._object_context_menu
        elif node_type in {"prefix", "bucket"}:
            menu = self._folder_context_menu
        if not menu:
            return "break"
        self._dismiss_context_menu()
        self._active_context_menu = menu
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()
        return "break"

    def _load_prefix_thread(self, node_id: str, bucket: str, prefix: str) -> None:
        try:
            listing = self.controller.list_objects(
                bucket_name=bucket,
                max_keys=self._current_max_keys,
                prefix=prefix,
            )
            self.root.after(0, lambda: self._render_prefix_listing(node_id, listing))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_prefix_error(node_id, msg))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_prefix_error(node_id, msg))

    def _load_more_thread(
        self,
        node_id: str,
        parent_id: str,
        bucket: str,
        prefix: str,
        delimiter: str | None,
        continuation_token: str,
    ) -> None:
        try:
            listing = self.controller.list_objects(
                bucket_name=bucket,
                max_keys=self._current_max_keys,
                prefix=prefix,
                delimiter=delimiter,
                continuation_token=continuation_token,
            )
            self.root.after(0, lambda: self._handle_load_more_result(node_id, parent_id, listing))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_load_more_error(node_id, msg))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_load_more_error(node_id, msg))

    def _render_prefix_listing(self, node_id: str, listing: BucketListing) -> None:
        node_info = self._node_state.get(node_id)
        if not node_info:
            return
        self._delete_child_nodes(node_id)
        if listing.error:
            self.results_tree.insert(node_id, "end", text=f"Error: {listing.error}")
            node_info["loading"] = False
            return

        objects_added, prefixes_added = self._render_listing_contents(node_id, listing)
        if not (objects_added or prefixes_added):
            placeholder = "(Empty)"
            if node_info.get("type") == "bucket":
                placeholder = "(No objects)"
            self.results_tree.insert(node_id, "end", text=placeholder)
        node_info["loaded"] = True
        node_info["loading"] = False
        prefix_label = listing.prefix or "/"
        self._set_status(
            f"Loaded {objects_added} object(s) and {prefixes_added} folder(s) under {prefix_label}."
        )

    def _handle_load_more_result(self, node_id: str, parent_id: str, listing: BucketListing) -> None:
        if self.results_tree.exists(node_id):
            self._delete_subtree(node_id)
        if not self.results_tree.exists(parent_id):
            return
        node_info = self._node_state.get(parent_id)
        objects_added, prefixes_added = self._render_listing_contents(parent_id, listing)
        if node_info and node_info.get("type") == "prefix":
            node_info["loaded"] = True
            node_info["loading"] = False
        prefix_label = listing.prefix or "/"
        self._set_status(
            f"Loaded {objects_added} more object(s) and {prefixes_added} more folder(s) under {prefix_label}."
        )

    def _handle_load_more_error(self, node_id: str, message: str) -> None:
        node_info = self._node_state.get(node_id)
        if not node_info:
            return
        node_info["loading"] = False
        self.results_tree.item(node_id, text="Load more...")
        self._show_error("List Error", f"Error loading more items: {message}")

    def _handle_prefix_error(self, node_id: str, message: str) -> None:
        node_info = self._node_state.get(node_id)
        if not node_info:
            return
        self._delete_child_nodes(node_id)
        self.results_tree.insert(node_id, "end", text=f"Error: {message}")
        node_info["loading"] = False
        prefix_label = node_info.get("prefix") or "/"
        self._show_error("List Error", f"Error loading {prefix_label}: {message}")

    def _show_object_details(self, bucket: str, key: str) -> None:
        dialog = ObjectDetailsDialog(
            self.root,
            bucket=bucket,
            key=key,
            on_download=lambda details=None: self._download_object(bucket, key, details),
            on_delete=lambda: self._delete_object(bucket, key),
            on_generate_url=lambda: self.open_signed_url_dialog(bucket=bucket, key=key),
        )
        thread = threading.Thread(
            target=self._load_object_details_thread,
            args=(dialog, bucket, key),
            daemon=True,
        )
        thread.start()
        dialog.show()

    def _load_object_details_thread(self, dialog: "ObjectDetailsDialog", bucket: str, key: str) -> None:
        try:
            details = self.controller.get_object_details(bucket_name=bucket, key=key)
            self.root.after(0, lambda: dialog.display_details(details))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: dialog.display_error(msg))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: dialog.display_error(msg))

    def upload_file(self) -> None:
        if not self.controller.is_connected:
            messagebox.showerror("Error", "Please connect first")
            return
        destination = self._get_selected_upload_target()
        bucket_name = destination[0] if destination else self.bucket_var.get()
        if not bucket_name:
            messagebox.showerror("Error", "Please select a bucket first")
            return
        default_prefix = destination[1] if destination else ""
        file_path = filedialog.askopenfilename(parent=self.root, title="Choose File to Upload")
        if not file_path:
            return
        try:
            file_size = os.path.getsize(file_path)
        except OSError as exc:
            messagebox.showerror("Error", f"Unable to read file: {exc}")
            return
        default_name = os.path.basename(file_path.rstrip(os.sep)) or os.path.basename(file_path)
        if not default_name:
            default_name = os.path.basename(file_path)
        dialog = UploadDialog(
            self.root,
            bucket=bucket_name,
            source_path=file_path,
            source_size=file_size,
            initial_prefix=default_prefix,
            initial_name=default_name or "",
        )
        result = dialog.show()
        if not result:
            return
        object_name = result["name"]
        prefix = result["prefix"]
        try:
            key = self._compose_s3_key(prefix, object_name)
        except ValueError as exc:
            messagebox.showerror("Error", str(exc))
            return
        self._set_status(f"Uploading {object_name} to {bucket_name}/{key}...")
        dialog = self._start_transfer_dialog(
            title="Uploading",
            description=f"Uploading to s3://{bucket_name}/{key}",
            total_bytes=file_size,
        )
        thread = threading.Thread(
            target=self._upload_object_thread,
            args=(bucket_name, key, file_path, dialog),
            daemon=True,
        )
        thread.start()

    def _download_object(self, bucket: str, key: str, details: ObjectDetails | None = None) -> None:
        initial_name = os.path.basename(key.rstrip("/")) or key
        destination = filedialog.asksaveasfilename(
            parent=self.root,
            title="Save Object As",
            initialfile=initial_name,
        )
        if not destination:
            return
        size_hint = details.size if details and details.size is not None else None

        def _begin_download(size: int | None) -> None:
            self._set_status(f"Downloading {key}...")
            dialog = self._start_transfer_dialog(
                title="Downloading",
                description=f"Downloading s3://{bucket}/{key}",
                total_bytes=size,
            )
            thread = threading.Thread(
                target=self._download_object_thread,
                args=(bucket, key, destination, dialog),
                daemon=True,
            )
            thread.start()

        if size_hint is not None:
            _begin_download(size_hint)
            return

        self._set_status(f"Preparing download for {key}...")

        def _fetch_size() -> None:
            size_value: int | None = None
            warning_message: str | None = None
            try:
                metadata = self.controller.get_object_details(bucket_name=bucket, key=key)
                size_value = metadata.size
            except (ClientError, BotoCoreError) as exc:
                warning_message = f"Unable to fetch object info: {exc}"
            except Exception as exc:  # pragma: no cover - defensive
                warning_message = f"Unable to prepare download: {exc}"

            def _finish_fetch(size=size_value, warning=warning_message) -> None:
                if warning:
                    messagebox.showwarning(
                        "Download",
                        f"{warning}\nContinuing download without size information.",
                    )
                _begin_download(size)

            self.root.after(0, _finish_fetch)

        threading.Thread(target=_fetch_size, daemon=True).start()

    def _delete_object(self, bucket: str, key: str) -> None:
        self._set_status(f"Deleting {key}...")
        thread = threading.Thread(
            target=self._delete_object_thread,
            args=(bucket, key),
            daemon=True,
        )
        thread.start()

    def _download_object_thread(self, bucket: str, key: str, destination: str, dialog: "TransferDialog") -> None:
        def progress(total: int) -> None:
            self._report_transfer_progress(dialog, total)

        try:
            self.controller.download_object(
                bucket_name=bucket,
                key=key,
                destination=destination,
                progress_callback=progress,
                cancel_requested=dialog.cancel_requested,
            )
            self.root.after(0, lambda: self._handle_download_success(key, destination, dialog))
        except TransferCancelledError:
            self.root.after(0, lambda: self._handle_transfer_cancelled(dialog, f"Download of {key} cancelled."))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_download_error(key, msg, dialog))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_download_error(key, msg, dialog))

    def _handle_download_success(self, key: str, destination: str, dialog: "TransferDialog" | None = None) -> None:
        self._close_transfer_dialog(dialog)
        messagebox.showinfo("Download Complete", f"Saved '{key}' to:\n{destination}")
        self.status_var.set(f"Downloaded {key} to {destination}")

    def _handle_download_error(self, key: str, message: str, dialog: "TransferDialog" | None = None) -> None:
        self._close_transfer_dialog(dialog)
        self._show_error("Download Error", f"Error downloading {key}: {message}")

    def _delete_object_thread(self, bucket: str, key: str) -> None:
        try:
            self.controller.delete_object(bucket_name=bucket, key=key)
            self.root.after(0, lambda: self._handle_delete_success(bucket, key))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_delete_error(key, msg))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_delete_error(key, msg))

    def _handle_delete_success(self, bucket: str, key: str) -> None:
        messagebox.showinfo("Delete Complete", f"Deleted s3://{bucket}/{key}")
        self.status_var.set(f"Deleted {key} from {bucket}")
        self._remove_object_from_tree(bucket, key)

    def _handle_delete_error(self, key: str, message: str) -> None:
        self._show_error("Delete Error", f"Error deleting {key}: {message}")

    def _upload_object_thread(self, bucket: str, key: str, source_path: str, dialog: "TransferDialog") -> None:
        def progress(total: int) -> None:
            self._report_transfer_progress(dialog, total)

        try:
            self.controller.upload_object(
                bucket_name=bucket,
                key=key,
                source_path=source_path,
                progress_callback=progress,
                cancel_requested=dialog.cancel_requested,
            )
            self.root.after(0, lambda: self._handle_upload_success(bucket, key, dialog))
        except TransferCancelledError:
            self.root.after(0, lambda: self._handle_transfer_cancelled(dialog, f"Upload of {key} cancelled."))
        except (ClientError, BotoCoreError) as exc:
            error_message = str(exc)
            self.root.after(0, lambda msg=error_message: self._handle_upload_error(key, msg, dialog))
        except Exception as exc:  # pragma: no cover - defensive
            message = f"Unexpected error: {exc}"
            self.root.after(0, lambda msg=message: self._handle_upload_error(key, msg, dialog))

    def _handle_upload_success(self, bucket: str, key: str, dialog: "TransferDialog" | None = None) -> None:
        self._close_transfer_dialog(dialog)
        messagebox.showinfo("Upload Complete", f"Uploaded to s3://{bucket}/{key}")
        self.status_var.set(f"Uploaded {key} to {bucket}")
        self._add_object_to_tree(bucket, key)

    def _handle_upload_error(self, key: str, message: str, dialog: "TransferDialog" | None = None) -> None:
        self._close_transfer_dialog(dialog)
        self._show_error("Upload Error", f"Error uploading {key}: {message}")

    def open_signed_url_dialog(self, bucket: str | None = None, key: str | None = None) -> None:
        if not self.controller.is_connected:
            messagebox.showerror("Error", "Please connect first")
            return
        selection = self._get_selected_object_path()
        bucket_name = bucket or (selection[0] if selection else self.bucket_var.get())
        key_name = key if key is not None else (selection[1] if selection else "")
        if not bucket_name:
            messagebox.showerror("Error", "Please select a bucket first")
            return
        dialog = SignedUrlDialog(
            self.root,
            initial_bucket=bucket_name or "",
            initial_key=key_name or "",
            default_max_size=self._app_settings.default_post_max_size,
            on_generate=self._generate_signed_url,
        )
        dialog.show()

    def _generate_signed_url(
        self,
        payload: dict,
        on_success: Callable[[str | dict[str, dict[str, str] | str]], None],
        on_error: Callable[[str], None],
    ) -> None:
        def task() -> None:
            try:
                url = self.controller.generate_presigned_url(**payload)
                self.root.after(0, lambda value=url: on_success(value))
            except (ClientError, BotoCoreError) as exc:
                message = str(exc)
                self.root.after(0, lambda msg=message: on_error(msg))
            except Exception as exc:  # pragma: no cover - defensive
                message = f"Unexpected error: {exc}"
                self.root.after(0, lambda msg=message: on_error(msg))

        thread = threading.Thread(target=task, daemon=True)
        thread.start()

    def _delete_child_nodes(self, parent_id: str) -> None:
        for child in self.results_tree.get_children(parent_id):
            self._delete_subtree(child)

    def _delete_subtree(self, node_id: str) -> None:
        for child in self.results_tree.get_children(node_id):
            self._delete_subtree(child)
        self.results_tree.delete(node_id)
        self._node_state.pop(node_id, None)

    def _clear_tree(self) -> None:
        for child in self.results_tree.get_children():
            self._delete_subtree(child)
        self._node_state.clear()
        selected_items = self.results_tree.selection()
        if selected_items:
            self.results_tree.selection_remove(*selected_items)
        self.results_tree.focus("")
        self._refresh_selection_controls()

    def _set_status(self, message: str) -> None:
        self.root.after(0, lambda: self.status_var.set(message))

    def _start_transfer_dialog(
        self,
        *,
        title: str,
        description: str,
        total_bytes: int | None = None,
    ) -> "TransferDialog":
        dialog = TransferDialog(
            self.root,
            title=title,
            description=description,
            total_bytes=total_bytes,
        )
        self._transfer_dialog = dialog
        return dialog

    def _close_transfer_dialog(self, dialog: "TransferDialog" | None) -> None:
        if not dialog:
            return
        dialog.close()
        if self._transfer_dialog is dialog:
            self._transfer_dialog = None

    def _report_transfer_progress(self, dialog: "TransferDialog", total: int) -> None:
        if not dialog:
            return

        def _update() -> None:
            dialog.update_progress(total)

        self.root.after(0, _update)

    def _handle_transfer_cancelled(self, dialog: "TransferDialog" | None, message: str) -> None:
        self._close_transfer_dialog(dialog)
        self._set_status(message)

    def _show_error(self, title: str, message: str) -> None:
        def _display() -> None:
            messagebox.showerror(title, message)
            self.status_var.set(message)

        self.root.after(0, _display)


class TransferDialog:
    """Modal dialog that displays transfer progress and offers cancellation."""

    def __init__(
        self,
        parent: tk.Tk,
        *,
        title: str,
        description: str,
        total_bytes: int | None = None,
    ):
        self.parent = parent
        self._total_bytes = total_bytes or 0
        self._indeterminate = not total_bytes or total_bytes <= 0
        self._transferred = 0
        self._cancel_requested = False
        self._closed = False

        self.top = tk.Toplevel(parent)
        self.top.title(title)
        self.top.transient(parent)
        self.top.resizable(False, False)
        self.top.grab_set()

        frame = ttk.Frame(self.top, padding="15")
        frame.grid(row=0, column=0, sticky=(tk.W, tk.E))
        frame.columnconfigure(0, weight=1)

        ttk.Label(frame, text=description, wraplength=360, justify="left").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )

        mode = "indeterminate" if self._indeterminate else "determinate"
        self.progress = ttk.Progressbar(frame, mode=mode, length=320)
        self.progress.grid(row=1, column=0, sticky=(tk.W, tk.E))
        if self._indeterminate:
            self.progress.start(10)
        else:
            self.progress.config(maximum=max(self._total_bytes, 1), value=0)

        self.progress_var = tk.StringVar(value="Preparing transfer...")
        ttk.Label(frame, textvariable=self.progress_var).grid(row=2, column=0, sticky=tk.W, pady=(5, 0))

        self.status_var = tk.StringVar(value="In progress...")
        ttk.Label(frame, textvariable=self.status_var, foreground="gray").grid(
            row=3, column=0, sticky=tk.W, pady=(5, 0)
        )

        buttons = ttk.Frame(frame)
        buttons.grid(row=4, column=0, sticky=tk.E, pady=(15, 0))
        self.cancel_button = ttk.Button(buttons, text="Cancel", command=self._on_cancel)
        self.cancel_button.grid(row=0, column=0)

        self.top.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def update_progress(self, transferred: int) -> None:
        if self._closed:
            return
        self._transferred = max(transferred, 0)
        if self._indeterminate:
            self.progress_var.set(f"{self._format_size(self._transferred)} transferred")
        else:
            maximum = max(self._total_bytes, 1)
            percent = min(self._transferred / maximum, 1.0)
            self.progress["value"] = min(self._transferred, maximum)
            total_label = self._format_size(self._total_bytes)
            self.progress_var.set(
                f"{self._format_size(self._transferred)} of {total_label} ({percent:.0%})"
            )
        if not self._cancel_requested:
            self.status_var.set("Transferring...")

    def cancel_requested(self) -> bool:
        return self._cancel_requested

    def set_status(self, message: str) -> None:
        if self._closed:
            return
        self.status_var.set(message)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._indeterminate:
            self.progress.stop()
        try:
            self.top.grab_release()
        except tk.TclError:
            pass
        self.top.destroy()

    def _on_cancel(self) -> None:
        if self._cancel_requested or self._closed:
            return
        self._cancel_requested = True
        self.status_var.set("Cancelling...")
        self.cancel_button.config(state="disabled")

    def _format_size(self, size: int) -> str:
        suffixes = ["B", "KB", "MB", "GB", "TB"]
        value = float(max(size, 0))
        for suffix in suffixes:
            if value < 1024 or suffix == suffixes[-1]:
                return f"{value:.1f} {suffix}" if suffix != "B" else f"{int(value)} {suffix}"
            value /= 1024
        return f"{size} B"


class ConnectionDialog:
    """Simple modal dialog for creating or editing connection profiles."""

    def __init__(
        self,
        parent: tk.Tk,
        *,
        title: str,
        profile: ConnectionProfile | None = None,
        primary_action: str = "save",
        primary_label: str | None = None,
    ):
        self.parent = parent
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

        self.top = tk.Toplevel(parent)
        self.top.title(title)
        self.top.transient(parent)
        self.top.resizable(False, False)
        self.top.grab_set()

        content = ttk.Frame(self.top, padding="10")
        content.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        self.name_var = tk.StringVar(value=profile.name if profile else "")
        self.endpoint_var = tk.StringVar(value=profile.endpoint_url if profile else "")
        self.access_key_var = tk.StringVar(value=profile.access_key if profile else "")
        self.secret_key_var = tk.StringVar(value=profile.secret_key if profile else "")

        ttk.Label(content, text="Name:").grid(row=0, column=0, sticky=tk.W, pady=2)
        ttk.Entry(content, textvariable=self.name_var, width=40).grid(
            row=0, column=1, sticky=(tk.W, tk.E), pady=2
        )

        ttk.Label(content, text="Endpoint URL:").grid(row=1, column=0, sticky=tk.W, pady=2)
        ttk.Entry(content, textvariable=self.endpoint_var, width=40).grid(
            row=1, column=1, sticky=(tk.W, tk.E), pady=2
        )

        ttk.Label(content, text="Access Key ID:").grid(row=2, column=0, sticky=tk.W, pady=2)
        ttk.Entry(content, textvariable=self.access_key_var, width=40).grid(
            row=2, column=1, sticky=(tk.W, tk.E), pady=2
        )

        ttk.Label(content, text="Secret Access Key:").grid(row=3, column=0, sticky=tk.W, pady=2)
        ttk.Entry(content, textvariable=self.secret_key_var, width=40, show="*").grid(
            row=3, column=1, sticky=(tk.W, tk.E), pady=2
        )

        buttons = ttk.Frame(content)
        buttons.grid(row=4, column=0, columnspan=2, pady=(10, 0), sticky=tk.E)

        self._primary_button = ttk.Button(buttons, text=self._save_label, command=self._on_save)
        self._primary_button.grid(row=0, column=0, padx=5)
        ttk.Button(buttons, text="Cancel", command=self._on_cancel).grid(row=0, column=1, padx=5)
        if profile:
            ttk.Button(buttons, text="Delete", command=self._on_delete).grid(row=0, column=2, padx=5)

        self.top.protocol("WM_DELETE_WINDOW", self._on_cancel)
        for var in (self.name_var, self.endpoint_var, self.access_key_var, self.secret_key_var):
            var.trace_add("write", self._on_field_change)
        self._update_primary_state()

    def show(self) -> dict | None:
        self.parent.wait_window(self.top)
        return self.result

    def _validate_fields(self) -> bool:
        if not self._fields_filled():
            messagebox.showerror("Error", "All fields are required")
            return False
        return True

    def _fields_filled(self) -> bool:
        return all(
            [
                self.name_var.get().strip(),
                self.endpoint_var.get().strip(),
                self.access_key_var.get().strip(),
                self.secret_key_var.get().strip(),
            ]
        )

    def _has_changes(self) -> bool:
        if not self._original_values:
            return True
        current = {
            "name": self.name_var.get().strip(),
            "endpoint_url": self.endpoint_var.get().strip(),
            "access_key": self.access_key_var.get().strip(),
            "secret_key": self.secret_key_var.get().strip(),
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

    def _on_field_change(self, *_: object) -> None:
        self._update_primary_state()

    def _update_primary_state(self) -> None:
        label = self._resolve_primary_label()
        state = "normal" if self._fields_filled() else "disabled"
        self._primary_button.configure(text=label, state=state)

    def _on_save(self) -> None:
        if not self._validate_fields():
            return
        action = self._resolve_primary_action()
        if action == "connect":
            target_name = self.original_name or self.name_var.get().strip()
            self.result = {"action": "connect", "name": target_name}
        else:
            profile = ConnectionProfile(
                name=self.name_var.get().strip(),
                endpoint_url=self.endpoint_var.get().strip(),
                access_key=self.access_key_var.get().strip(),
                secret_key=self.secret_key_var.get().strip(),
            )
            self.result = {"action": action, "profile": profile, "original_name": self.original_name}
        self.top.destroy()

    def _on_delete(self) -> None:
        if not self.original_name:
            return
        confirmed = messagebox.askyesno(
            "Delete Connection",
            f"Delete connection '{self.original_name}'?",
            parent=self.top,
        )
        if not confirmed:
            return
        self.result = {"action": "delete", "name": self.original_name}
        self.top.destroy()

    def _on_cancel(self) -> None:
        self.result = None
        self.top.destroy()


class ObjectDetailsDialog:
    """Modal dialog that loads and displays metadata for an object."""

    def __init__(
        self,
        parent: tk.Tk,
        *,
        bucket: str,
        key: str,
        on_download: Callable[[ObjectDetails | None], None] | None = None,
        on_delete: Callable[[], None] | None = None,
        on_generate_url: Callable[[], None] | None = None,
    ):
        self.parent = parent
        self.bucket = bucket
        self.key = key
        self._closed = False
        self._details: ObjectDetails | None = None
        self._on_download = on_download
        self._on_delete = on_delete
        self._on_generate_url = on_generate_url

        self.top = tk.Toplevel(parent)
        self.top.title("Object Details")
        self.top.transient(parent)
        self.top.resizable(False, False)
        self.top.withdraw()

        content = ttk.Frame(self.top, padding="10")
        content.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        path = f"s3://{bucket}/{key}"
        ttk.Label(content, text="Path:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.path_var = tk.StringVar(value=path)
        path_entry = ttk.Entry(content, textvariable=self.path_var, width=55, state="readonly")
        path_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=2)

        self.status_var = tk.StringVar(value="Loading metadata...")
        ttk.Label(content, textvariable=self.status_var).grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(5, 2))

        self.progress = ttk.Progressbar(content, mode="indeterminate")
        self.progress.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E))
        self.progress.start()

        self.details_frame = ttk.Frame(content, padding=(0, 10, 0, 0))
        self.details_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E))
        self.details_frame.columnconfigure(1, weight=1)
        self.details_frame.grid_remove()

        self._detail_vars = {
            "Bucket": tk.StringVar(value=bucket),
            "Key": tk.StringVar(value=key),
            "Size": tk.StringVar(value="-"),
            "Last modified": tk.StringVar(value="-"),
            "Storage class": tk.StringVar(value="-"),
            "ETag": tk.StringVar(value="-"),
            "Content type": tk.StringVar(value="-"),
        }
        for idx, (label, var) in enumerate(self._detail_vars.items()):
            ttk.Label(self.details_frame, text=f"{label}:").grid(row=idx, column=0, sticky=tk.W, pady=2)
            entry = ttk.Entry(self.details_frame, textvariable=var, width=40, state="readonly")
            entry.grid(row=idx, column=1, sticky=(tk.W, tk.E), pady=2)

        checksums_row = len(self._detail_vars)
        ttk.Label(self.details_frame, text="Checksums:").grid(
            row=checksums_row, column=0, sticky=tk.NW, pady=(10, 0)
        )
        self.checksums_text = tk.Text(
            self.details_frame,
            width=45,
            height=4,
            wrap="word",
        )
        self.checksums_text.grid(row=checksums_row, column=1, sticky=(tk.W, tk.E), pady=(10, 0))
        self.checksums_text.bind("<Key>", self._on_metadata_key)

        metadata_row = checksums_row + 1
        ttk.Label(self.details_frame, text="Metadata:").grid(
            row=metadata_row, column=0, sticky=tk.NW, pady=(10, 0)
        )
        self.metadata_text = tk.Text(
            self.details_frame,
            width=45,
            height=8,
            wrap="word",
        )
        self.metadata_text.grid(row=metadata_row, column=1, sticky=(tk.W, tk.E), pady=(10, 0))
        self.metadata_text.bind("<Key>", self._on_metadata_key)

        buttons = ttk.Frame(content)
        buttons.grid(row=4, column=0, columnspan=2, pady=(10, 0), sticky=tk.E)
        column = 0
        if self._on_download:
            ttk.Button(buttons, text="Download", command=self._handle_download).grid(row=0, column=column, padx=5)
            column += 1
        if self._on_delete:
            ttk.Button(buttons, text="Delete", command=self._handle_delete).grid(row=0, column=column, padx=5)
            column += 1
        if self._on_generate_url:
            ttk.Button(buttons, text="Signed URL", command=self._handle_signed_url).grid(
                row=0, column=column, padx=5
            )
            column += 1
        ttk.Button(buttons, text="Close", command=self.close).grid(row=0, column=column, padx=5)

        self.top.protocol("WM_DELETE_WINDOW", self.close)
        self.top.update_idletasks()
        self.top.deiconify()
        self.top.wait_visibility()
        self.top.grab_set()

    def show(self) -> None:
        self.parent.wait_window(self.top)

    def display_details(self, details: ObjectDetails) -> None:
        if self._closed:
            return
        self._details = details
        self.progress.stop()
        self.progress.grid_remove()
        self.details_frame.grid()
        self.status_var.set("Metadata loaded.")
        self._detail_vars["Bucket"].set(details.bucket)
        self._detail_vars["Key"].set(details.key)
        self._detail_vars["Size"].set(self._format_size(details.size))
        self._detail_vars["Last modified"].set(self._format_last_modified(details.last_modified))
        self._detail_vars["Storage class"].set(details.storage_class or "-")
        self._detail_vars["ETag"].set(details.etag or "-")
        self._detail_vars["Content type"].set(details.content_type or "-")
        checksums_value = "\n".join(f"{k}: {v}" for k, v in sorted(details.checksums.items())) or "None"
        self._set_checksums_text(checksums_value)
        metadata_value = "\n".join(f"{k}: {v}" for k, v in sorted(details.metadata.items())) or "None"
        self._set_metadata_text(metadata_value)

    def display_error(self, message: str) -> None:
        if self._closed:
            return
        self.progress.stop()
        self.progress.grid_remove()
        self.details_frame.grid_remove()
        self.status_var.set(f"Error loading metadata: {message}")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.progress.stop()
        self.top.destroy()

    def _set_metadata_text(self, text: str) -> None:
        self.metadata_text.delete("1.0", tk.END)
        self.metadata_text.insert("1.0", text)
        self.metadata_text.see("1.0")

    def _set_checksums_text(self, text: str) -> None:
        self.checksums_text.delete("1.0", tk.END)
        self.checksums_text.insert("1.0", text)
        self.checksums_text.see("1.0")

    def _format_size(self, size: int | None) -> str:
        if size is None:
            return "-"
        suffixes = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        for suffix in suffixes:
            if value < 1024 or suffix == suffixes[-1]:
                return f"{value:.1f} {suffix}" if suffix != "B" else f"{int(value)} {suffix}"
            value /= 1024
        return f"{size} B"

    def _format_last_modified(self, last_modified) -> str:
        if not last_modified:
            return "-"
        try:
            return last_modified.strftime("%Y-%m-%d %H:%M:%S %Z").strip() or last_modified.isoformat()
        except AttributeError:
            return str(last_modified)

    def _on_metadata_key(self, event) -> str | None:
        if (event.state & 0x4) and event.keysym.lower() in {"c", "a"}:
            return None
        return "break"

    def _handle_download(self) -> None:
        if not self._on_download:
            return
        self._on_download(self._details)

    def _handle_delete(self) -> None:
        if not self._on_delete:
            return
        confirmed = messagebox.askyesno(
            "Delete Object",
            f"Delete '{self.key}' from bucket '{self.bucket}'?",
            parent=self.top,
        )
        if not confirmed:
            return
        self.close()
        self._on_delete()

    def _handle_signed_url(self) -> None:
        if not self._on_generate_url:
            return
        self._on_generate_url()


class UploadDialog:
    """Modal dialog used to confirm upload details before starting the transfer."""

    def __init__(
        self,
        parent: tk.Tk,
        *,
        bucket: str,
        source_path: str,
        source_size: int,
        initial_prefix: str,
        initial_name: str,
    ):
        self.parent = parent
        self.bucket = bucket
        self.result: dict | None = None

        self.top = tk.Toplevel(parent)
        self.top.title("Upload File")
        self.top.transient(parent)
        self.top.resizable(False, False)
        self.top.grab_set()

        content = ttk.Frame(self.top, padding="10")
        content.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        content.columnconfigure(1, weight=1)

        ttk.Label(content, text="Bucket:").grid(row=0, column=0, sticky=tk.W, pady=2)
        ttk.Label(content, text=f"s3://{bucket}").grid(row=0, column=1, sticky=tk.W, pady=2)

        ttk.Label(content, text="Source file:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.source_var = tk.StringVar(value=source_path)
        ttk.Entry(content, textvariable=self.source_var, state="readonly", width=55).grid(
            row=1, column=1, sticky=(tk.W, tk.E), pady=2
        )

        ttk.Label(content, text="File size:").grid(row=2, column=0, sticky=tk.W, pady=2)
        ttk.Label(content, text=self._format_size(source_size)).grid(row=2, column=1, sticky=tk.W, pady=2)

        ttk.Label(content, text="Destination folder:").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.prefix_var = tk.StringVar(value=initial_prefix or "")
        ttk.Entry(content, textvariable=self.prefix_var, width=45).grid(row=3, column=1, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(content, text="Object name:").grid(row=4, column=0, sticky=tk.W, pady=2)
        self.name_var = tk.StringVar(value=initial_name or "")
        ttk.Entry(content, textvariable=self.name_var, width=45).grid(row=4, column=1, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(content, text="Resulting path:").grid(row=5, column=0, sticky=tk.W, pady=(5, 2))
        self.full_path_var = tk.StringVar()
        ttk.Label(content, textvariable=self.full_path_var).grid(row=5, column=1, sticky=tk.W, pady=(5, 2))
        self._update_full_path()

        self.prefix_var.trace_add("write", lambda *_: self._update_full_path())
        self.name_var.trace_add("write", lambda *_: self._update_full_path())

        buttons = ttk.Frame(content)
        buttons.grid(row=6, column=0, columnspan=2, pady=(10, 0), sticky=tk.E)
        ttk.Button(buttons, text="Upload", command=self._on_upload).grid(row=0, column=0, padx=5)
        ttk.Button(buttons, text="Cancel", command=self._on_cancel).grid(row=0, column=1, padx=5)

        self.top.protocol("WM_DELETE_WINDOW", self._on_cancel)

    def show(self) -> dict | None:
        self.parent.wait_window(self.top)
        return self.result

    def _on_cancel(self) -> None:
        self.result = None
        self.top.destroy()

    def _on_upload(self) -> None:
        name = self.name_var.get().strip()
        if not name:
            messagebox.showerror("Error", "Please provide an object name")
            return
        prefix = self.prefix_var.get().strip()
        self.result = {"prefix": prefix, "name": name}
        self.top.destroy()

    def _update_full_path(self) -> None:
        prefix = self.prefix_var.get().strip()
        name = self.name_var.get().strip()
        cleaned_prefix = prefix.rstrip("/")
        if cleaned_prefix:
            cleaned_prefix += "/"
        path = f"{cleaned_prefix}{name}" if name else cleaned_prefix
        if path:
            self.full_path_var.set(f"s3://{self.bucket}/{path}")
        else:
            self.full_path_var.set(f"s3://{self.bucket}/")

    def _format_size(self, size: int) -> str:
        suffixes = ["B", "KB", "MB", "GB", "TB"]
        value = float(size)
        for suffix in suffixes:
            if value < 1024 or suffix == suffixes[-1]:
                return f"{value:.1f} {suffix}" if suffix != "B" else f"{int(value)} {suffix}"
            value /= 1024
        return f"{size} B"


class SignedUrlDialog:
    """Dialog to gather inputs and display a generated signed URL."""

    def __init__(
        self,
        parent: tk.Tk,
        *,
        initial_bucket: str,
        initial_key: str,
        default_max_size: int,
        on_generate: Callable[
            [dict, Callable[[str | dict[str, dict[str, str] | str]], None], Callable[[str], None]],
            None,
        ],
        initial_method: str = "get",
    ):
        self.parent = parent
        self._on_generate_request = on_generate
        self._generated = False
        self._in_progress = False
        self._closed = False

        self.top = tk.Toplevel(parent)
        self.top.title("Generate Signed URL")
        self.top.transient(parent)
        self.top.resizable(False, False)
        self.top.grab_set()

        content = ttk.Frame(self.top, padding="10")
        content.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        content.columnconfigure(1, weight=1)

        self.bucket_var = tk.StringVar(value=initial_bucket or "")
        self.key_var = tk.StringVar(value=initial_key or "")
        method_value = (initial_method or "get").strip().lower() or "get"
        if method_value not in {"get", "put", "post"}:
            method_value = "get"
        self.method_var = tk.StringVar(value=method_value)
        self.post_mode_var = tk.StringVar(value="single")
        self.expires_var = tk.StringVar(value="3600")
        default_size_value, default_size_unit = _split_size_bytes(default_max_size)
        self.max_size_var = tk.StringVar(value=default_size_value)
        self.max_size_unit_var = tk.StringVar(value=default_size_unit)
        self.content_type_var = tk.StringVar()
        self.content_disposition_var = tk.StringVar()
        self.full_path_var = tk.StringVar()
        self.status_var = tk.StringVar(value="")

        ttk.Label(content, text="Bucket:").grid(row=0, column=0, sticky=tk.W, pady=2)
        self.bucket_entry = ttk.Entry(content, textvariable=self.bucket_var, width=40, state="readonly")
        self.bucket_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(content, text="Object key:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.key_entry = ttk.Entry(content, textvariable=self.key_var, width=40)
        self.key_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(content, text="Full path:").grid(row=2, column=0, sticky=tk.W, pady=(2, 6))
        ttk.Label(content, textvariable=self.full_path_var).grid(row=2, column=1, sticky=tk.W, pady=(2, 6))

        ttk.Label(content, text="Operation:").grid(row=3, column=0, sticky=tk.W, pady=2)
        method_frame = ttk.Frame(content)
        method_frame.grid(row=3, column=1, sticky=tk.W, pady=2)
        self.method_radios = [
            ttk.Radiobutton(method_frame, text="GET", value="get", variable=self.method_var),
            ttk.Radiobutton(method_frame, text="PUT", value="put", variable=self.method_var),
            ttk.Radiobutton(method_frame, text="POST", value="post", variable=self.method_var),
        ]
        for idx, radio in enumerate(self.method_radios):
            radio.grid(row=0, column=idx, padx=(0, 10))

        self.post_mode_label = ttk.Label(content, text="POST key mode:")
        self.post_mode_label.grid(row=4, column=0, sticky=tk.W, pady=2)
        self.post_mode_frame = ttk.Frame(content)
        self.post_mode_frame.grid(row=4, column=1, sticky=tk.W, pady=2)
        self.post_mode_radios = [
            ttk.Radiobutton(self.post_mode_frame, text="Single file", value="single", variable=self.post_mode_var),
            ttk.Radiobutton(self.post_mode_frame, text="Key prefix", value="prefix", variable=self.post_mode_var),
        ]
        for idx, radio in enumerate(self.post_mode_radios):
            radio.grid(row=0, column=idx, padx=(0, 10))

        self.max_size_label = ttk.Label(content, text="Max file size:")
        self.max_size_label.grid(row=5, column=0, sticky=tk.W, pady=2)
        self.max_size_frame = ttk.Frame(content)
        self.max_size_frame.grid(row=5, column=1, sticky=tk.W, pady=2)
        self.max_size_entry = ttk.Entry(self.max_size_frame, textvariable=self.max_size_var, width=10, justify="right")
        self.max_size_entry.grid(row=0, column=0, sticky=tk.W)
        self.max_size_unit = ttk.Combobox(
            self.max_size_frame,
            textvariable=self.max_size_unit_var,
            values=SIZE_UNITS,
            state="readonly",
            width=6,
        )
        self.max_size_unit.grid(row=0, column=1, sticky=tk.W, padx=(6, 0))

        ttk.Label(content, text="Expiration (seconds):").grid(row=6, column=0, sticky=tk.W, pady=2)
        self.expires_entry = ttk.Entry(content, textvariable=self.expires_var, width=15, justify="right")
        self.expires_entry.grid(row=6, column=1, sticky=tk.W, pady=2)

        ttk.Label(content, text="Content-Type (optional):").grid(row=7, column=0, sticky=tk.W, pady=2)
        self.content_type_entry = ttk.Entry(content, textvariable=self.content_type_var, width=40)
        self.content_type_entry.grid(row=7, column=1, sticky=(tk.W, tk.E), pady=2)

        ttk.Label(content, text="Content-Disposition (optional):").grid(row=8, column=0, sticky=tk.W, pady=2)
        self.content_disposition_entry = ttk.Entry(
            content, textvariable=self.content_disposition_var, width=40
        )
        self.content_disposition_entry.grid(row=8, column=1, sticky=(tk.W, tk.E), pady=2)

        self.url_frame = ttk.Frame(content)
        self.url_frame.grid(row=9, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        self.url_frame.columnconfigure(0, weight=1)
        ttk.Label(self.url_frame, text="Signed URL:").grid(row=0, column=0, sticky=tk.W)
        self.url_text = tk.Text(self.url_frame, width=55, height=5, wrap="word")
        self.url_text.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(2, 2))
        self.url_text.bind("<Key>", self._on_readonly_text_key)
        ttk.Button(self.url_frame, text="Copy URL", command=self._copy_url).grid(row=1, column=1, sticky=tk.N, padx=(5, 0))
        self.url_frame.grid_remove()

        self.wget_frame = ttk.Frame(content)
        self.wget_frame.grid(row=10, column=0, columnspan=2, sticky=(tk.W, tk.E))
        self.wget_frame.columnconfigure(0, weight=1)
        ttk.Label(self.wget_frame, text="wget command:").grid(row=0, column=0, sticky=tk.W)
        self.wget_text = tk.Text(self.wget_frame, width=55, height=3, wrap="word")
        self.wget_text.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(2, 2))
        self.wget_text.bind("<Key>", self._on_readonly_text_key)
        ttk.Button(
            self.wget_frame, text="Copy wget", command=lambda: self._copy_from_text(self.wget_text, "wget command copied.")
        ).grid(row=1, column=1, sticky=tk.N, padx=(5, 0))
        self.wget_frame.grid_remove()

        self.curl_frame = ttk.Frame(content)
        self.curl_frame.grid(row=11, column=0, columnspan=2, sticky=(tk.W, tk.E))
        self.curl_frame.columnconfigure(0, weight=1)
        ttk.Label(self.curl_frame, text="curl command:").grid(row=0, column=0, sticky=tk.W)
        self.curl_text = tk.Text(self.curl_frame, width=55, height=3, wrap="word")
        self.curl_text.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(2, 2))
        self.curl_text.bind("<Key>", self._on_readonly_text_key)
        ttk.Button(
            self.curl_frame, text="Copy curl", command=lambda: self._copy_from_text(self.curl_text, "curl command copied.")
        ).grid(row=1, column=1, sticky=tk.N, padx=(5, 0))
        self.curl_frame.grid_remove()

        buttons = ttk.Frame(content)
        buttons.grid(row=12, column=0, columnspan=2, pady=(10, 0), sticky=tk.E)
        self.generate_button = ttk.Button(buttons, text="Generate", command=self._on_generate)
        self.generate_button.grid(row=0, column=0, padx=5)
        ttk.Button(buttons, text="Close", command=self.close).grid(row=0, column=1, padx=5)

        self.bucket_var.trace_add("write", lambda *_: self._update_full_path())
        self.key_var.trace_add("write", lambda *_: self._update_full_path())
        self.method_var.trace_add("write", lambda *_: self._toggle_post_options())
        self._update_full_path()
        self._toggle_post_options()

        self.top.protocol("WM_DELETE_WINDOW", self.close)
        self.key_entry.focus()

    def show(self) -> None:
        self.parent.wait_window(self.top)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.top.destroy()

    def _update_full_path(self) -> None:
        bucket = self.bucket_var.get().strip()
        key = self.key_var.get().strip()
        if bucket and key:
            self.full_path_var.set(f"s3://{bucket}/{key}")
        elif bucket:
            self.full_path_var.set(f"s3://{bucket}/")
        else:
            self.full_path_var.set("s3://")

    def _toggle_post_options(self) -> None:
        is_post = (self.method_var.get().strip().lower() or "get") == "post"
        for widget in (self.post_mode_label, self.post_mode_frame, self.max_size_label, self.max_size_frame):
            if is_post:
                widget.grid()
            else:
                widget.grid_remove()

    def _on_generate(self) -> None:
        if self._generated or self._in_progress:
            return
        bucket = self.bucket_var.get().strip()
        key = self.key_var.get().strip()
        if not bucket:
            messagebox.showerror("Error", "Bucket is required", parent=self.top)
            return
        if not key:
            messagebox.showerror("Error", "Object key is required", parent=self.top)
            return
        try:
            expires = int(self.expires_var.get().strip())
        except ValueError:
            messagebox.showerror("Error", "Expiration must be a whole number", parent=self.top)
            return
        if expires <= 0:
            messagebox.showerror("Error", "Expiration must be greater than zero", parent=self.top)
            return

        method = self.method_var.get().strip().lower() or "get"
        max_size = None
        post_key_mode = "single"
        if method == "post":
            post_key_mode = self.post_mode_var.get().strip().lower() or "single"
            max_size = _parse_size_bytes(self.max_size_var.get(), self.max_size_unit_var.get())
            if max_size is None:
                messagebox.showerror("Error", "Max file size must be a whole number greater than zero", parent=self.top)
                return

        content_type = self.content_type_var.get().strip() or None
        content_disposition = self.content_disposition_var.get().strip() or None
        payload = {
            "bucket_name": bucket,
            "key": key,
            "method": method,
            "expires_in": expires,
            "content_type": content_type,
            "content_disposition": content_disposition,
            "post_key_mode": post_key_mode,
            "max_size": max_size,
        }

        self._in_progress = True
        self.generate_button.config(state="disabled")
        self._on_generate_request(payload, self._handle_generate_success, self._handle_generate_error)

    def _handle_generate_success(self, result: str | dict[str, dict[str, str] | str]) -> None:
        if self._closed:
            return
        self._generated = True
        self._in_progress = False
        self._lock_fields()
        if isinstance(result, dict):
            url = str(result.get("url", ""))
            fields = result.get("fields")
            post_fields = fields if isinstance(fields, dict) else None
            self._display_url(url, post_fields=post_fields)
        else:
            self._display_url(result)

    def _handle_generate_error(self, message: str) -> None:
        if self._closed:
            return
        self._in_progress = False
        messagebox.showerror("Signed URL", f"Error generating URL: {message}", parent=self.top)
        if not self._generated:
            self.generate_button.config(state="normal")

    def _lock_fields(self) -> None:
        for entry in (
            self.bucket_entry,
            self.key_entry,
            self.expires_entry,
            self.max_size_entry,
            self.content_type_entry,
            self.content_disposition_entry,
        ):
            entry.config(state="readonly")
        for radio in self.method_radios:
            radio.config(state="disabled")
        for radio in self.post_mode_radios:
            radio.config(state="disabled")
        self.max_size_unit.config(state="disabled")
        self.generate_button.config(state="disabled")

    def _display_url(self, url: str, post_fields: dict[str, str] | None = None) -> None:
        method = self.method_var.get().strip().lower() or "get"
        if method == "post":
            self.url_frame.grid_remove()
        else:
            self._show_text_block(self.url_frame, self.url_text, url)
        wget_cmd, curl_cmd = self._build_command_texts(url, post_fields=post_fields)
        if wget_cmd:
            self._show_text_block(self.wget_frame, self.wget_text, wget_cmd)
        else:
            self.wget_frame.grid_remove()
        if curl_cmd:
            self._show_text_block(self.curl_frame, self.curl_text, curl_cmd)
        else:
            self.curl_frame.grid_remove()

    def _copy_url(self) -> None:
        self._copy_from_text(self.url_text, "Signed URL copied to clipboard.")

    def _copy_from_text(self, widget: tk.Text, message: str) -> None:
        value = widget.get("1.0", tk.END).strip()
        if not value:
            return
        self.parent.clipboard_clear()
        self.parent.clipboard_append(value)
        self.status_var.set(message)

    def _show_text_block(self, frame: ttk.Frame, widget: tk.Text, value: str) -> None:
        frame.grid()
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert("1.0", value)
        widget.see("1.0")

    def _build_command_texts(
        self,
        url: str,
        *,
        post_fields: dict[str, str] | None = None,
    ) -> tuple[str | None, str | None]:
        method = self.method_var.get().strip().lower() or "get"
        filename = self._suggest_command_filename()
        if method == "get":
            wget_cmd = f'wget "{url}" -O "{filename}"'
            curl_cmd = f'curl -L "{url}" -o "{filename}"'
            return wget_cmd, curl_cmd

        if method == "post":
            fields = post_fields or {}
            curl_parts = ["curl", "-X", "POST"]
            ordered_keys = [key for key in ("key",) if key in fields]
            ordered_keys.extend(sorted(key for key in fields.keys() if key not in ordered_keys))
            for key in ordered_keys:
                curl_parts.append(f'-F "{key}={fields[key]}"')
            curl_parts.append('-F "file=@PATH_TO_FILE"')
            curl_parts.append(f'"{url}"')
            return None, " ".join(curl_parts)

        headers: list[tuple[str, str]] = []
        content_type = self.content_type_var.get().strip()
        if content_type:
            headers.append(("Content-Type", content_type))
        content_disposition = self.content_disposition_var.get().strip()
        if content_disposition:
            headers.append(("Content-Disposition", content_disposition))

        wget_parts = ["wget", "--method=PUT", f'--body-file="{filename}"']
        curl_parts = ["curl", f'-T "{filename}"']
        for name, value in headers:
            wget_parts.append(f'--header="{name}: {value}"')
            curl_parts.append(f'-H "{name}: {value}"')
        wget_parts.append(f'"{url}"')
        curl_parts.append(f'"{url}"')
        return " ".join(wget_parts), " ".join(curl_parts)

    def _suggest_command_filename(self) -> str:
        key = self.key_var.get().strip().rstrip("/")
        if not key:
            return "local-file"
        name = key.rsplit("/", 1)[-1]
        return name or "local-file"

    def _on_readonly_text_key(self, event) -> str | None:
        if (event.state & 0x4) and event.keysym.lower() in {"c", "a"}:
            return None
        return "break"
