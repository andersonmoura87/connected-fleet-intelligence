"""Identifier rules used as path segments."""

import pytest

from cfi.domain.identifiers import validate_identifier


@pytest.mark.parametrize("value", ["synthetic.trips.v1", "v1", "a0", "trip_2024-06"])
def test_accepts_safe_identifiers(value: str) -> None:
    assert validate_identifier(value) == value


@pytest.mark.parametrize(
    "value",
    ["", ".", "..", "../x", "a/b", "a\\b", "a.", "CON", "aux", "COM1", "LPT9", " has-space"],
)
def test_rejects_unsafe_identifiers(value: str) -> None:
    with pytest.raises(ValueError, match="invalid_identifier"):
        validate_identifier(value)
