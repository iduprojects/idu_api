"""
Object geometries data table is defined here
"""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table

from urban_api.db import metadata

object_geometries_data_id_seq = Sequence("object_geometries_data_id_seq")

object_geometries_data = Table(
    "object_geometries_data",
    metadata,
    Column("object_geometry_id", Integer, primary_key=True, server_default=object_geometries_data_id_seq.next_value()),
    Column("territory_id", ForeignKey("territories_data.territory_id"), nullable=True),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column(
        "centre_point",
        Geometry("POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("address", String(300)),
)

"""
Object geometries:
- object_geometry_id int 
- territory_id foreign key int
- geometry geometry 
- centre_point geometry point
- address string(300)
"""
