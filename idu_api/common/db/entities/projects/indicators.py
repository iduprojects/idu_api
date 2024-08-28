from sqlalchemy import Column, ForeignKey, Integer, Table, Float, Enum, Date

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import DateFieldType

DateFieldTypeEnum = Enum(DateFieldType, name="date_field_type")

indicators_data = Table(
    "indicators_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id"),
        nullable=False,
    ),
    Column(
        "indicator_id",
        Integer,
        ForeignKey("indicators_dict.indicator_id"),
        nullable=False,
    ),
    Column("date_type", DateFieldTypeEnum, nullable=False),
    Column("date_value", Date, nullable=False),
    Column("value", Float(53), nullable=False),
    schema="user_projects",
)

"""
Indicators data:
- scenario_id foreign key int
- indicator_id foreign key int
- date_type enum
- date_value date
- value float
"""
