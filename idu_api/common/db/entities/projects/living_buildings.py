from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

living_buildings_id_seq = Sequence("living_buildings_id_seq", schema="user_projects")

projects_living_buildings_data = Table(
    "living_buildings_data",
    metadata,
    Column("living_building_id", Integer, primary_key=True, server_default=living_buildings_id_seq.next_value()),
    Column("physical_object_id", Integer, ForeignKey("physical_objects_data.physical_object_id"), nullable=False),
    Column("residental_number", Integer, nullable=False, unique=False),
    Column("living_area", Integer, nullable=False, unique=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    schema="user_projects",
)

"""
Living buildings data:
- living_building_id int 
- physical_object_id foreign key int
- residental_number int
- living_area int
- properties jsonb
"""
