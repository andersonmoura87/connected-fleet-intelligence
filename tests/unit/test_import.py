"""Import and version checks for the cfi package."""

from cfi import __version__


def test_package_imports() -> None:
    assert __version__ == "0.1.0a0"
