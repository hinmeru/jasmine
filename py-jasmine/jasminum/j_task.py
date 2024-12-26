from datetime import datetime

from .j import J


class JTask:
    function: J
    # jType.LIST
    args: list[J]
    start_time: datetime
    end_time: datetime | None
    interval: int
    last_run: datetime
    next_run: datetime
    is_active: bool
    description: str
    upd_time: datetime

    def __init__(
        self,
        function: J,
        args: J,
        start_time: datetime,
        end_time: datetime | None,
        interval: int,
        description: str,
    ):
        self.function = function
        self.args = args
        self.start_time = start_time
        self.end_time = end_time
        self.interval = interval
        self.description = description
        self.last_run = None
        self.next_run = start_time
        self.is_active = True
        self.upd_time = datetime.now()
