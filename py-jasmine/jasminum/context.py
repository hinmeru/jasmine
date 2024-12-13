class Context:
    locals: dict[str, any]
    handles: dict[int, any]

    def __init__(self, locals: dict) -> None:
        self.locals = locals
        self.handles = dict()
