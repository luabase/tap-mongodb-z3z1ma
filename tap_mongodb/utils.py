from __future__ import annotations

import base64
import io
import paramiko
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

def guess_key_type(key_data: str) -> paramiko.PKey:
    """Guess the type of the private key.
    Taken from postgres-tap: https://github.com/MeltanoLabs/tap-postgres/blob/d8228de8230af7d6dabad3cb088f203500d742b0/tap_postgres/tap.py#L459

    We are duplicating some logic from the ssh_tunnel package here,
    we could try to use their function instead.

    Args:
        key_data: The private key data to guess the type of.

    Returns:
        The private key object.

    Raises:
        ValueError: If the key type could not be determined.
    """
    for key_class in (
        paramiko.RSAKey,
        paramiko.DSSKey,
        paramiko.ECDSAKey,
        paramiko.Ed25519Key,
    ):
        try:
            key = key_class.from_private_key(io.StringIO(key_data))
        except paramiko.SSHException:  # noqa: PERF203
            continue
        else:
            return key

    errmsg = "Could not determine the key type."
    raise ValueError(errmsg)
