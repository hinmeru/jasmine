from .exceptions import JasmineEvalException
from .j import J


def show(j: J) -> None:
    print(j)
    return j


def assert_eq(j1: J, j2: J) -> None:
    if j1 != j2:
        raise JasmineEvalException("assert failed: %s != %s" % (j1, j2))
    return J(None)


def assert_true(j: J) -> None:
    if not j.to_bool():
        raise JasmineEvalException("assert failed: %s is not true" % j)
    return J(None)
