from __future__ import annotations
"""UI-agnostic helpers for formatting and command generation."""
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import PackageNotFoundError, metadata, version

DIST_NAME = "pys3b"
SIZE_UNITS = ("B", "KB", "MB", "GB")
SIZE_UNIT_FACTORS = {
    "B": 1,
    "KB": 1024,
    "MB": 1024 * 1024,
    "GB": 1024 * 1024 * 1024,
}


@dataclass(frozen=True)
class PackageInfo:
    name: str
    version: str
    summary: str
    homepage: str | None
    repository: str | None
    author: str | None


def load_package_info(dist_name: str = DIST_NAME) -> PackageInfo:
    try:
        distribution_metadata = metadata(dist_name)
        package_version = version(dist_name)
    except PackageNotFoundError:
        return PackageInfo(
            name="S3 Object Browser",
            version="",
            summary="Browse buckets and objects stored in Amazon S3.",
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


def split_size_bytes(size_bytes: int) -> tuple[str, str]:
    if size_bytes <= 0:
        return ("1", "MB")
    for unit in ("GB", "MB", "KB"):
        factor = SIZE_UNIT_FACTORS[unit]
        if size_bytes >= factor and size_bytes % factor == 0:
            return (str(size_bytes // factor), unit)
    return (str(size_bytes), "B")


def parse_size_bytes(value: str, unit: str) -> int | None:
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


def format_size(size: int | None) -> str:
    if size is None:
        return "-"
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(size, 0))
    for suffix in suffixes:
        if value < 1024 or suffix == suffixes[-1]:
            return f"{value:.1f} {suffix}" if suffix != "B" else f"{int(value)} {suffix}"
        value /= 1024
    return f"{size} B"


def format_last_modified(last_modified: object) -> str:
    if not last_modified:
        return "-"
    if isinstance(last_modified, datetime):
        return last_modified.strftime("%Y-%m-%d %H:%M:%S %Z").strip() or last_modified.isoformat()
    try:
        return last_modified.strftime("%Y-%m-%d %H:%M:%S %Z").strip() or str(last_modified)
    except AttributeError:
        return str(last_modified)


def compose_s3_key(prefix: str, name: str) -> str:
    key_name = name.strip()
    if not key_name:
        raise ValueError("Object name cannot be empty")
    cleaned_prefix = prefix.strip().lstrip("/")
    if cleaned_prefix and not cleaned_prefix.endswith("/"):
        cleaned_prefix += "/"
    return f"{cleaned_prefix}{key_name}" if cleaned_prefix else key_name


def suggest_command_filename(key: str) -> str:
    cleaned = key.strip().rstrip("/")
    if not cleaned:
        return "local-file"
    name = cleaned.rsplit("/", 1)[-1]
    return name or "local-file"


def build_signed_url_commands(
    *,
    method: str,
    url: str,
    filename: str,
    content_type: str | None = None,
    content_disposition: str | None = None,
    post_fields: dict[str, str] | None = None,
) -> tuple[str | None, str | None]:
    normalized = (method or "get").strip().lower()
    if normalized == "get":
        wget_cmd = f'wget "{url}" -O "{filename}"'
        curl_cmd = f'curl -L "{url}" -o "{filename}"'
        return wget_cmd, curl_cmd

    if normalized == "post":
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
    if content_type:
        headers.append(("Content-Type", content_type))
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
