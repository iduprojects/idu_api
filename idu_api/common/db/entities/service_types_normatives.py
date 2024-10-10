"""
Service types normatives data table is defined here
"""

from typing import Callable

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    func,
)

from idu_api.common.db import metadata

func: Callable

service_types_normatives_data_id_seq = Sequence("service_types_normatives_data_id_seq")

service_types_normatives_data = Table(
    "service_types_normatives_data",
    metadata,
    Column("normative_id", Integer, primary_key=True, server_default=service_types_normatives_data_id_seq.next_value()),
    Column("service_type_id", ForeignKey("service_types_dict.service_type_id", ondelete="CASCADE")),
    Column("urban_function_id", ForeignKey("urban_functions_dict.urban_function_id", ondelete="CASCADE")),
    Column("territory_id", ForeignKey("territories_data.territory_id")),
    Column("is_regulated", Boolean, nullable=False),
    Column("radius_availability_meters", Integer),
    Column("time_availability_minutes", Integer),
    Column("services_per_1000_normative", Float(53)),
    Column("services_capacity_per_1000_normative", Float(53)),
    Column("source", String(300)),
    Column("year", Integer, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    UniqueConstraint(
        "service_type_id",
        "territory_id",
        "year",
        name="service_types_normatives_data_service_type_territory_key",  # postgres max column name len = 63
    ),
    UniqueConstraint(
        "urban_function_id",
        "territory_id",
        "year",
        name="service_types_normatives_data_urban_func_territory_key",  # postgres max column name len = 63
    ),
)

"""
Service types normatives:
- normative_id int 
- service_type_id foreign key int
- urban_function_id foreign key int
- territory_id foreign key int
- is_regulated bool
- radius_availability_meters int
- time_availability_minutes int
- services_per_1000_normative float(53)
- services_capacity_per_1000_normative float(53)
- source string(300)
- year int
- created_at timestamp
- updated_at timestamp
"""
