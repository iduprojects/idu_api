"""Projects living buildings data table is defined here."""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.projects.physical_objects import projects_physical_objects_data

living_buildings_id_seq = Sequence("living_buildings_id_seq", schema="user_projects")

projects_living_buildings_data = Table(
    "living_buildings_data",
    metadata,
    Column("living_building_id", Integer, primary_key=True, server_default=living_buildings_id_seq.next_value()),
    Column(
        "physical_object_id",
        Integer,
        ForeignKey(projects_physical_objects_data.c.physical_object_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column("living_area", Integer, nullable=True),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    schema="user_projects",
)

"""
Living buildings data:
- living_building_id int 
- physical_object_id foreign key int
- living_area int
- properties jsonb
"""
