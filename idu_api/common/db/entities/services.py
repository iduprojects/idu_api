"""
Services data table is defined here
"""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

services_data_id_seq = Sequence("services_data_id_seq")

services_data = Table(
    "services_data",
    metadata,
    Column("service_id", Integer, primary_key=True, server_default=services_data_id_seq.next_value()),
    Column("service_type_id", ForeignKey("service_types_dict.service_type_id"), nullable=False),
    Column("territory_type_id", ForeignKey("territory_types_dict.territory_type_id")),
    Column("name", String(200)),
    Column("capacity_real", Integer),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
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
