"""
Service types normatives data table is defined here
"""

from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, Sequence, Table, UniqueConstraint

from urban_api.db import metadata

service_types_normatives_data_id_seq = Sequence("service_types_normatives_data_id_seq")

service_types_normatives_data = Table(
    "service_types_normatives_data",
    metadata,
    Column("normative_id", Integer, primary_key=True, server_default=service_types_normatives_data_id_seq.next_value()),
    Column("service_type_id", ForeignKey("service_types_dict.service_type_id"), nullable=False),
    Column("urban_function_id", ForeignKey("urban_functions_dict.urban_function_id"), nullable=False),
    Column("territory_id", ForeignKey("territories_data.territory_id"), nullable=False),
    Column("is_regulated", Boolean, nullable=False),
    Column("radius_availability_meters", Integer),
    Column("time_availability_minutes", Integer),
    Column("services_per_1000_normative", Float(53)),
    Column("services_capacity_per_1000_normative", Float(53)),
    UniqueConstraint(
        "service_type_id",
        "urban_function_id",
        "territory_id",
        name="service_types_normatives_data_service_type_id_urban_functio_key",  # postgres max column name len = 63
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
"""
