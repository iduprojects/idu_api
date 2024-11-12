"""Projects services data table is defined here."""

from typing import Callable

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.service_types import service_types_dict
from idu_api.common.db.entities.services import services_data
from idu_api.common.db.entities.territories import territory_types_dict

func: Callable

services_id_seq = Sequence("services_id_seq", schema="user_projects")

projects_services_data = Table(
    "services_data",
    metadata,
    metadata,
    Column("service_id", Integer, primary_key=True, server_default=services_id_seq.next_value()),
    Column(
        "public_service_id",
        Integer,
        ForeignKey(services_data.c.service_id, ondelete="SET NULL"),
        nullable=True,
    ),
    Column("service_type_id", ForeignKey(service_types_dict.c.service_type_id), nullable=False),
    Column("territory_type_id", ForeignKey(territory_types_dict.c.territory_type_id)),
    Column("name", String(200)),
    Column("capacity_real", Integer),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Services data:
- service_id int 
- public_service_id foreign key int
- service_type_id foreign key int
- territory_type_id foreign key int
- name str
- capacity_real int
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""
