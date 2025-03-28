"""Territory indicators data table is defined here."""

from typing import Callable

from sqlalchemy import TIMESTAMP, Column, Date, Enum, Float, ForeignKey, Integer, Sequence, String, Table, func

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import DateFieldType, IndicatorValueType
from idu_api.common.db.entities.indicators_dict import indicators_dict
from idu_api.common.db.entities.territories import territories_data

func: Callable

DateFieldTypeEnum = Enum(DateFieldType, name="date_field_type")
IndicatorValueTypeEnum = Enum(IndicatorValueType, name="indicator_value_type")

territory_indicators_data_id_seq = Sequence("territory_indicators_data_id_seq")
territory_indicators_data = Table(
    "territory_indicators_data",
    metadata,
    Column(
        "indicator_value_id",
        Integer,
        primary_key=True,
        server_default=territory_indicators_data_id_seq.next_value(),
        nullable=True,
    ),
    Column("indicator_id", ForeignKey(indicators_dict.c.indicator_id), nullable=False),
    Column("territory_id", ForeignKey(territories_data.c.territory_id), nullable=False),
    Column("date_type", DateFieldTypeEnum, nullable=False),
    Column("date_value", Date, nullable=False),
    Column("value_type", IndicatorValueTypeEnum, nullable=False),
    Column("information_source", String(300), nullable=False),
    Column("value", Float(53), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Territory indicators:
- indicator_value_id int
- indicator_id foreign key int 
- territory_id foreign key int
- date_type enum
- date_value date
- value_type enum
- information_source string(300)
- value float(53)
- created_at timestamp
- updated_at timestamp
"""
