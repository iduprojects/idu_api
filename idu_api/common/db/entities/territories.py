"""
Territories data table is defined here
"""

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

territories_data_id_seq = Sequence("territories_data_id_seq")

territories_data = Table(
    "territories_data",
    metadata,
    Column("territory_id", Integer, primary_key=True, server_default=territories_data_id_seq.next_value()),
    Column("territory_type_id", ForeignKey("territory_types_dict.territory_type_id"), nullable=False),
    Column("parent_id", ForeignKey("territories_data.territory_id"), nullable=True),
    Column("name", String(200), nullable=False),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("level", Integer, nullable=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "centre_point",
        Geometry("POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("admin_center", Integer),
    Column("okato_code", String(20)),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Territories:
- territory_id int 
- physical_object_type_id foreign key int
- parent_id foreign key int
- name string(200)
- geometry geometry 
- level int
- properties jsonb
- centre_point geometry point
- admin_center int
- okato_code string(20)
"""
