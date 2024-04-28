"""
Enumerations converted to database datatypes are defined here
"""

from enum import Enum


class DateFieldType(Enum):
    """
    Enumeration of date field types.
    """

    year = "year"
    half_year = "half_year"
    quarter = "quarter"
    month = "month"
    day = "day"


class IndicatorValueType(Enum):
    """
    Enumeration of indicator value types.
    """

    real = "real"
    forecast = "forecast"
    target = "target"
