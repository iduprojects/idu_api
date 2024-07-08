from enum import Enum


class DateType(str, Enum):
    YEAR = "year"
    HALF_YEAR = "half_year"
    QUARTER = "quarter"
    MONTH = "month"
    DAY = "day"


class Ordering(str, Enum):
    ASC = "asc"
    DESC = "desc"
