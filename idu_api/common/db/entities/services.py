"""
Services data table is defined here
"""

from typing import Callable

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.urban_types_dicts import service_types_dict, territory_types_dict

func: Callable

services_data_id_seq = Sequence("services_data_id_seq")

services_data = Table(
    "services_data",
    metadata,
    Column("service_id", Integer, primary_key=True, server_default=services_data_id_seq.next_value()),
    Column("service_type_id", ForeignKey(service_types_dict.c.service_type_id), nullable=False),
    Column("territory_type_id", ForeignKey(territory_types_dict.c.territory_type_id)),
    Column("name", String(200)),
    Column("capacity_real", Integer),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Services:
- service_id int 
- service_type_id foreign key int
- territory_type_id foreign key int
- name string(200)
- capacity_real int
- properties jsonb
"""
