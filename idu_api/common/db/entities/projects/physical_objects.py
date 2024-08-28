from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

physical_objects_id_seq = Sequence("physical_objects_id_seq", schema="user_projects")

projects_physical_objects_data = Table(
    "physical_objects_data",
    metadata,
    Column("physical_object_id", Integer, primary_key=True, server_default=physical_objects_id_seq.next_value()),
    Column(
        "physical_object_type_id",
        Integer,
        ForeignKey("physical_object_types_dict.physical_object_type_id"),
        nullable=False,
    ),
    Column("name", String(200), nullable=False, unique=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("address", String(300), nullable=True),
    schema="user_projects",
)

"""
Physical objects data:
- physical_object_id int 
- physical_object_type_id foreign key int
- name str
- properties jsonb
- address str
"""
