"""Projects living buildings data table is defined here."""

from sqlalchemy import Column, Float, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.projects.physical_objects import projects_physical_objects_data

buildings_id_seq = Sequence("buildings_data_id_seq", schema="user_projects")

projects_buildings_data = Table(
    "buildings_data",
    metadata,
    Column("building_id", Integer, primary_key=True, server_default=buildings_id_seq.next_value()),
    Column(
        "physical_object_id",
        Integer,
        ForeignKey(projects_physical_objects_data.c.physical_object_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("floors", Integer(), nullable=True),
    Column("building_area_modeled", Float(precision=53), nullable=True),
    Column("building_area_official", Float(precision=53), nullable=True),
    Column("project_type", String(length=512), nullable=True),
    Column("floor_type", String(length=128), nullable=True),
    Column("wall_material", String(length=128), nullable=True),
    Column("built_year", Integer(), nullable=True),
    Column("exploitation_start_year", Integer(), nullable=True),
    schema="user_projects",
)

"""
Living buildings:
- living_building_id int 
- physical_object_type_id foreign key int
- properties jsonb
- floors int
- building_area_modeled float
- building_area_official float
- project_type string
- floor_type string
- wall_material string
- built_year int
- exploitation_start_year int
"""
