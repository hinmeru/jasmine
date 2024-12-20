from .j import J


class Context:
    locals: dict[str, J]
    handles: dict[int, any]

    def __init__(self, locals: dict) -> None:
        self.locals = locals
        self.handles = dict()

    def set_var(self, key: str, value: J) -> None:
        self.locals[key] = value

    def get_var(self, key: str) -> J:
        return self.locals[key]

    def has_var(self, key: str) -> bool:
        return key in self.locals

    def set_handle(self, key: int, value: any) -> None:
        self.handles[key] = value

    def get_handle(self, key: int) -> any:
        return self.handles[key]
