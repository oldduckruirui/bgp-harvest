"""Helpers for deriving stable identifiers from AS paths."""

from __future__ import annotations

import hashlib
from typing import Sequence


def compute_path_id(as_path: Sequence[int]) -> int:
    """Compute a deterministic 64-bit identifier for an AS path."""

    serialized = ",".join(str(item) for item in as_path)
    digest = hashlib.sha256(serialized.encode("ascii")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)
