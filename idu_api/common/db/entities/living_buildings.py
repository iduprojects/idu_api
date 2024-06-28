"""
Living buildings data table is defined here
"""

from sqlalchemy import Column, Float, ForeignKey, Integer, Sequence, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

living_buildings_data_id_seq = Sequence("living_buildings_data_id_seq")

living_buildings_data = Table(
    "living_buildings_data",
    metadata,
    Column("living_building_id", Integer, primary_key=True, server_default=living_buildings_data_id_seq.next_value()),
    Column("physical_object_id", ForeignKey("physical_objects_data.physical_object_id"), nullable=False),
    Column("residents_number", Integer),
    Column("living_area", Float(53)),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
)

"""
Living buildings:
- living_building_id int 
- physical_object_type_id foreign key int
- residents_number int
- living_area float(53)
- properties jsonb
"""
