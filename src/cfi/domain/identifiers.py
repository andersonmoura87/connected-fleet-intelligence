"""Path-safe identifiers for Bronze source identity."""

import re

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_RESERVED = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)


def validate_identifier(value: str) -> str:
    """Return value when it can be one path segment. Reject traversal and device names."""

    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None or value.endswith("."):
        raise ValueError("invalid_identifier")
    if value.upper() in _RESERVED:
        raise ValueError("invalid_identifier")
    return value
