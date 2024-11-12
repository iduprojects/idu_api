"""Projects physical objects data table is defined here."""

from typing import Callable

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.physical_object_types import physical_object_types_dict
from idu_api.common.db.entities.physical_objects import physical_objects_data

func: Callable

physical_objects_id_seq = Sequence("physical_objects_id_seq", schema="user_projects")

projects_physical_objects_data = Table(
    "physical_objects_data",
    metadata,
    Column("physical_object_id", Integer, primary_key=True, server_default=physical_objects_id_seq.next_value()),
    Column(
        "public_physical_object_id",
        Integer,
        ForeignKey(physical_objects_data.c.physical_object_id, ondelete="SET NULL"),
        nullable=True,
    ),
    Column(
        "physical_object_type_id",
        Integer,
        ForeignKey(physical_object_types_dict.c.physical_object_type_id),
        nullable=False,
    ),
    Column("name", String(300)),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Physical objects data:
- physical_object_id int 
- public_physical_object_id foreign key int
- physical_object_type_id foreign key int
- name str
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""
