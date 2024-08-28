from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

services_id_seq = Sequence("services_id_seq", schema="user_projects")

services_data = Table(
    "services_data",
    metadata,
    Column("service_id", Integer, primary_key=True, server_default=services_id_seq.next_value()),
    Column("service_type_id", Integer, ForeignKey("service_types_dict.service_type_id"), nullable=False),
    Column("name", String(200), nullable=False, unique=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("list_label", String(20), nullable=False),
    Column("capacity_real", Integer, nullable=False),
    schema="user_projects",
)

"""
Services data:
- service_id int 
- service_type_id foreign key int
- name str
- properties jsonb
- list_label str
- capacity_real int
"""
