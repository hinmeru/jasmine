import contextlib

with contextlib.suppress(ImportError):
    from jasminum.jasminum import JasmineError, JasmineParseError


class JasmineEvalException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


__all__ = [JasmineError, JasmineParseError, JasmineEvalException]
