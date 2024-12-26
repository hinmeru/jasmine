from .exceptions import JasmineEvalException
from .j import J, JType


def validate_args(args: list[J], arg_types: list[JType]) -> None:
    if len(args) != len(arg_types):
        raise JasmineEvalException("invalid number of arguments")
    for i, arg in enumerate(args):
        if isinstance(arg_types[i], list) and arg.j_type not in arg_types[i]:
            raise JasmineEvalException(
                "invalid %s argument type: %s, expected: %s"
                % (
                    i + 1,
                    arg.j_type,
                    "|".join([a.name for a in arg_types[i]]),
                )
            )
        elif isinstance(arg_types[i], JType):
            if arg.j_type == JType.NULL:
                continue
            elif arg.j_type != arg_types[i]:
                raise JasmineEvalException(
                    "invalid %s argument type: %s, expected: %s"
                    % (i + 1, arg.j_type, arg_types[i])
                )
