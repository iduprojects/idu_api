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


class OrderByField(str, Enum):
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class NormativeType(str, Enum):
    SELF = "self"
    PARENT = "parent"
    GLOBAL = "global"


class InfrastructureType(str, Enum):
    BASIC = "basic"
    ADDITIONAL = "additional"
    COMFORT = "comfort"


class ProjectType(str, Enum):
    COMMON = "common"
    CITY = "city"


class ProjectPhase(str, Enum):
    INVESTMENT = "investment"
    PRE_DESIGN = "pre_design"
    DESIGN = "design"
    CONSTRUCTION = "construction"
    OPERATION = "operation"
    DECOMMISSION = "decommission"
