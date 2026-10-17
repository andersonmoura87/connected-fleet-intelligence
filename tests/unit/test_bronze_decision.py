"""Pure Bronze decisions. These tests do not touch the filesystem."""

from cfi.domain.bronze import IngestAction, ObservationKind, decide


def test_absent_source_publishes() -> None:
    assert decide(ObservationKind.ABSENT, None, "a" * 64) is IngestAction.PUBLISH


def test_matching_seal_is_noop() -> None:
    digest = "b" * 64
    assert decide(ObservationKind.SEALED, digest, digest) is IngestAction.NOOP


def test_different_bytes_conflict() -> None:
    assert decide(ObservationKind.SEALED, "a" * 64, "b" * 64) is IngestAction.CONFLICT


def test_incomplete_and_mismatch_fail_closed() -> None:
    assert decide(ObservationKind.INCOMPLETE, None, "a" * 64) is IngestAction.INCOMPLETE
    assert decide(ObservationKind.MISMATCH, "a" * 64, "a" * 64) is IngestAction.MISMATCH
