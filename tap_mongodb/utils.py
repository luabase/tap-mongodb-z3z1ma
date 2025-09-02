from __future__ import annotations

import base64
from typing import Any

from bson.binary import Binary


def _coerce_bytelikes_to_base64(value: Any) -> Any:
    """
    Recursively convert bytelike values to base64-encoded strings.

    Purpose:
    - Singer SDK's JSON serialization expects JSON-safe types. Raw bytes are not
      JSON-serializable and can cause UnicodeDecodeError when encoded as UTF-8.
    - MongoDB documents can contain bytelike fields (e.g., bson.binary.Binary).
      This helper ensures all such values are converted to base64 strings before
      emission, without altering schemas downstream.

    Behavior:
    - bytes/bytearray/memoryview/`bson.binary.Binary` -> base64 ASCII string
    - dict/list/tuple are traversed recursively
    - all other values are returned unchanged
    """

    if isinstance(value, (bytes, bytearray, memoryview, Binary)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, dict):
        return {k: _coerce_bytelikes_to_base64(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_bytelikes_to_base64(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_coerce_bytelikes_to_base64(v) for v in value)
    return value
