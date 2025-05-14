"""Tables which represent social groups and values are defined here.

Current list is:
- soc_groups_dict
- soc_values_dict
- soc_group_values_data
- soc_group_value_indicators_data
- soc_values_service_types_dict
"""

from collections.abc import Callable

from sqlalchemy import (
    TIMESTAMP,
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    func,
)

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import IndicatorValueType, InfrastructureType
from idu_api.common.db.entities.service_types import service_types_dict
from idu_api.common.db.entities.territories import territories_data

func: Callable

soc_groups_dict_id_seq = Sequence("soc_groups_dict_id_seq")
soc_groups_dict = Table(
    "soc_groups_dict",
    metadata,
    Column("soc_group_id", Integer, primary_key=True, server_default=soc_groups_dict_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=True),
)

"""
Social groups:
- soc_group_id int 
- name string(200)
"""


soc_values_dict_id_seq = Sequence("soc_values_dict_id_seq")
soc_values_dict = Table(
    "soc_values_dict",
    metadata,
    Column("soc_value_id", Integer, primary_key=True, server_default=soc_values_dict_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=True),
    Column("rank", Integer),
    Column("normative_value", Float),
    Column("decree_value", Float),
)

"""
Social values:
- soc_value_id int 
- name string(200)
- rank int
- normative_value float
- decree_value float
"""


InfrastructureTypeEnum = Enum(InfrastructureType, name="infrastructure_type")
soc_group_values_data_id_seq = Sequence("soc_group_values_data_id_seq")
soc_group_values_data = Table(
    "soc_group_values_data",
    metadata,
    Column("soc_group_value_id", Integer, primary_key=True, server_default=soc_group_values_data_id_seq.next_value()),
    Column("soc_group_id", ForeignKey(soc_groups_dict.c.soc_group_id, ondelete="CASCADE"), nullable=False),
    Column("service_type_id", ForeignKey(service_types_dict.c.service_type_id, ondelete="CASCADE"), nullable=False),
    Column("soc_value_id", ForeignKey(soc_values_dict.c.soc_value_id, ondelete="CASCADE"), nullable=True),
    Column("infrastructure_type", InfrastructureTypeEnum, default=InfrastructureType.basic, nullable=False),
)

"""
Social group value:
- soc_group_value_id int 
- soc_group_id foreign key int
- service_type_id foreign key int
- soc_value_id foreign key int
- infrastructure_type enum
"""

IndicatorValueTypeEnum = Enum(IndicatorValueType, name="indicator_value_type")
soc_group_value_indicators_data = Table(
    "soc_group_value_indicators_data",
    metadata,
    Column("soc_value_id", ForeignKey(soc_values_dict.c.soc_value_id, ondelete="CASCADE"), nullable=False),
    Column("territory_id", ForeignKey(territories_data.c.territory_id, ondelete="CASCADE"), nullable=False),
    Column("year", Integer, nullable=False),
    Column("value", Float(53), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    PrimaryKeyConstraint("soc_value_id", "territory_id", "year"),
)

"""
Social group indicator value:
- soc_value_id foreign key int
- territory_id foreign key int
- year int
- value float
- created_at timestamp
- updated_at timestamp
"""

soc_values_service_types_dict = Table(
    "soc_values_service_types_dict",
    metadata,
    Column("soc_value_id", ForeignKey(soc_values_dict.c.soc_value_id, ondelete="CASCADE"), nullable=False),
    Column("service_type_id", ForeignKey(service_types_dict.c.service_type_id, ondelete="CASCADE"), nullable=False),
    PrimaryKeyConstraint("soc_value_id", "service_type_id"),
)

"""
soc_values_service_types_dict:
- soc_value_id int
- service_type_id int
"""
