from __future__ import annotations

import base64
import json
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any
from uuid import UUID


def terminal_bytes(value: object) -> bytes:
    return json.dumps(
        _encode_terminal_value(value),
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _encode_terminal_value(item: Any) -> Any:
    if is_dataclass(item) and not isinstance(item, type):
        return {key: _encode_terminal_value(child) for key, child in asdict(item).items()}
    if isinstance(item, dict):
        return {str(key): _encode_terminal_value(child) for key, child in item.items()}
    if isinstance(item, tuple | list):
        return [_encode_terminal_value(child) for child in item]
    if isinstance(item, Enum):
        return item.value
    if isinstance(item, UUID):
        return str(item)
    if isinstance(item, bytes):
        return {
            "base64": base64.b64encode(item).decode(),
            "length": len(item),
        }
    return item
