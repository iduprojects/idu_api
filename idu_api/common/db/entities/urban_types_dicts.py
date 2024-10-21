"""
Table which represent all urban types are defined here.

Current list is:
- functional_zone_types_dict
- physical_object_types_dict
- territory_types_dict
- service_types_dict
"""

from sqlalchemy import Column, Enum, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import InfrastructureType

functional_zone_types_dict_id_seq = Sequence("functional_zone_types_dict_id_seq")

functional_zone_types_dict = Table(
    "functional_zone_types_dict",
    metadata,
    Column(
        "functional_zone_type_id",
        Integer,
        primary_key=True,
        server_default=functional_zone_types_dict_id_seq.next_value(),
    ),
    Column("name", String(100), nullable=False, unique=True),
    Column("zone_nickname", String(100)),
    Column("description", Text),
)

"""
Functional zone types:
- functional_zone_type_id int 
- name string(100)
- zone_nickname string(100)
- description text
"""

physical_object_types_dict_id_seq = Sequence("physical_object_types_dict_id_seq")

physical_object_types_dict = Table(
    "physical_object_types_dict",
    metadata,
    Column(
        "physical_object_type_id",
        Integer,
        primary_key=True,
        server_default=physical_object_types_dict_id_seq.next_value(),
    ),
    Column("name", String(200), nullable=False, unique=True),
    Column(
        "physical_object_function_id",
        Integer,
        ForeignKey("physical_object_functions_dict.physical_object_function_id"),
        nullable=True,
    ),
)

"""
Physical object types:
- physical_object_type_id int 
- name string(200)
- physical_object_function_id foreign key int
"""

territory_types_dict_id_seq = Sequence("territory_types_dict_id_seq")

territory_types_dict = Table(
    "territory_types_dict",
    metadata,
    Column("territory_type_id", Integer, primary_key=True, server_default=territory_types_dict_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=True),
)

"""
Territory types:
- territory_type_id int 
- name string(200)
"""

service_types_dict_id_seq = Sequence("service_types_dict_id_seq")
InfrastructureTypeEnum = Enum(InfrastructureType, name="infrastructure_type")

service_types_dict = Table(
    "service_types_dict",
    metadata,
    Column("service_type_id", Integer, primary_key=True, server_default=service_types_dict_id_seq.next_value()),
    Column("urban_function_id", ForeignKey("urban_functions_dict.urban_function_id"), nullable=False),
    Column("name", String(200), nullable=False, unique=True),
    Column("capacity_modeled", Integer),
    Column("code", String(50), nullable=False),
    Column("infrastructure_type", InfrastructureTypeEnum, default=InfrastructureType.basic, nullable=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
)

"""
Service types:
- service_type_id int 
- urban_function_id foreign key int
- name string(200)
- capacity_modeled int
- code string(50)
- infrastructure_type enum
- properties jsonb
"""
