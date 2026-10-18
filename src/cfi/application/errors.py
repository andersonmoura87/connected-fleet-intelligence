"""Failures reported by local cfi commands."""


class IngestFailure(Exception):
    def __init__(self, code: str, exit_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


def usage(code: str) -> IngestFailure:
    return IngestFailure(code, 2)


def integrity(code: str) -> IngestFailure:
    return IngestFailure(code, 3)


def storage(code: str) -> IngestFailure:
    return IngestFailure(code, 4)


def lock_unavailable() -> IngestFailure:
    return IngestFailure("lock_unavailable", 5)
