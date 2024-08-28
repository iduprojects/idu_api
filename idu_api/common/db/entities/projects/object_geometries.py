from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table

from idu_api.common.db import metadata

object_geometries_id_seq = Sequence("object_geometries_id_seq", schema="user_projects")

object_geometries_data = Table(
    "object_geometries_data",
    metadata,
    Column("object_geometry_id", Integer, primary_key=True, server_default=object_geometries_id_seq.next_value()),
    Column(
        "territory_id",
        Integer,
        ForeignKey("territories_data.territory_id"),
        nullable=False,
    ),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry"),
        nullable=False,
    ),
    Column(
        "centre_point",
        Geometry("POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry"),
        nullable=False,
    ),
    schema="user_projects",
)

"""
Object geometries data:
- object_geometry_id int 
- territory_id foreign key int
- geometry geometry
- centre_point geometry point
"""
