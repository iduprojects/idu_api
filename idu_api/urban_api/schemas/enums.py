from enum import Enum


class DateType(str, Enum):
    YEAR = "year"
    HALF_YEAR = "half_year"
    QUARTER = "quarter"
    MONTH = "month"
    DAY = "day"


class ValueType(str, Enum):
    REAL = "real"
    TARGET = "target"
    FORECAST = "forecast"


class Ordering(str, Enum):
    ASC = "asc"
    DESC = "desc"


class NormativeType(Enum):
    SELF = "self"
    PARENT = "parent"
    GLOBAL = "global"
