"""Stable content hashing for lineage and Bronze deduplication (C-4, C-5)."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def content_hash(data: dict[str, Any] | bytes | str) -> str:
    """Return a stable SHA-256 hex digest.

    # DESIGN-NOTE: dict keys are sorted before JSON serialisation so identical
    # content produces identical hashes regardless of insertion order (C-5 determinism).
    """
    if isinstance(data, bytes):
        raw = data
    elif isinstance(data, str):
        raw = data.encode("utf-8")
    else:
        raw = json.dumps(data, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
