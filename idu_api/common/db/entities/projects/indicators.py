from sqlalchemy import Column, Date, Enum, Float, ForeignKey, Integer, String, Table

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import DateFieldType, IndicatorValueType

DateFieldTypeEnum = Enum(DateFieldType, name="date_field_type", schema="user_projects")
IndicatorValueTypeEnum = Enum(IndicatorValueType, name="indicator_value_type", schema="user_projects")

projects_indicators_data = Table(
    "indicators_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id"),
        primary_key=True,
        nullable=False,
    ),
    Column(
        "indicator_id",
        Integer,
        ForeignKey("indicators_dict.indicator_id"),
        primary_key=True,
        nullable=False,
    ),
    Column("date_type", DateFieldTypeEnum, primary_key=True, nullable=False),
    Column("date_value", Date, primary_key=True, nullable=False),
    Column("value", Float(53), nullable=False),
    Column("value_type", IndicatorValueTypeEnum, primary_key=True, nullable=False),
    Column("information_source", String(300), primary_key=True, nullable=False),
    schema="user_projects",
)

"""
Indicators data:
- scenario_id foreign key int
- indicator_id foreign key int
- date_type enum
- date_value date
- value float
- value_type enum
- information_source string(300)
"""
