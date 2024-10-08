from sqlalchemy import Column, Date, Enum, Float, ForeignKey, Integer, String, Table

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import DateFieldType
from idu_api.common.db.entities.indicators_dict import indicators_dict
from idu_api.common.db.entities.projects.scenarios import scenarios_data

DateFieldTypeEnum = Enum(DateFieldType, name="date_field_type")

projects_indicators_data = Table(
    "indicators_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey(scenarios_data.c.scenario_id),
        nullable=False,
    ),
    Column(
        "indicator_id",
        Integer,
        ForeignKey(indicators_dict.c.indicator_id),
        nullable=False,
    ),
    Column("date_type", DateFieldTypeEnum, nullable=False),
    Column("date_value", Date, nullable=False),
    Column("value", Float(53), nullable=False),
    Column("comment", String(300)),
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
