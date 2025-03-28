"""Tables which represent service types are defined here.

Current list is:
- urban_functions_dict
- service_types_dict
"""

from sqlalchemy import Column, Enum, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import InfrastructureType

urban_functions_dict_id_seq = Sequence("urban_functions_dict_id_seq")
urban_functions_dict = Table(
    "urban_functions_dict",
    metadata,
    Column("urban_function_id", Integer, primary_key=True, server_default=urban_functions_dict_id_seq.next_value()),
    Column("parent_id", ForeignKey("urban_functions_dict.urban_function_id", ondelete="CASCADE")),
    Column("name", String(200), nullable=False, unique=True),
    Column("level", Integer, nullable=False),
    Column("list_label", String(20), nullable=False, unique=True),
    Column("code", String(50), nullable=False),
)

"""
Urban functions dict:
- urban_function_id int 
- parent_id foreign key int
- name string(200)
- level int
- list_label string(20)
- code string(50)
"""

service_types_dict_id_seq = Sequence("service_types_dict_id_seq")
InfrastructureTypeEnum = Enum(InfrastructureType, name="infrastructure_type")

service_types_dict = Table(
    "service_types_dict",
    metadata,
    Column("service_type_id", Integer, primary_key=True, server_default=service_types_dict_id_seq.next_value()),
    Column(
        "urban_function_id", ForeignKey(urban_functions_dict.c.urban_function_id, ondelete="CASCADE"), nullable=False
    ),
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
