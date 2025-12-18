from __future__ import annotations
"""Data models representing S3 listings."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class ObjectPage:
    """Represents a single page of S3 objects."""

    number: int
    keys: list[str] = field(default_factory=list)
    prefixes: list[str] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class BucketListing:
    """Represents the listing result for a bucket."""

    name: str
    prefix: str = ""
    delimiter: str = "/"
    pages: list[ObjectPage] = field(default_factory=list)
    error: Optional[str] = None
    has_more: bool = False
    continuation_token: Optional[str] = None


@dataclass
class ObjectDetails:
    """Metadata about a single S3 object."""

    bucket: str
    key: str
    size: Optional[int] = None
    last_modified: Optional[datetime] = None
    storage_class: Optional[str] = None
    etag: Optional[str] = None
    content_type: Optional[str] = None
    metadata: dict[str, str] = field(default_factory=dict)
