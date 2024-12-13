from datetime import date


def date_to_num(dt: date) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day
