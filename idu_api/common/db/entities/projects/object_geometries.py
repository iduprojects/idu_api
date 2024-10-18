from typing import Callable

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, func

from idu_api.common.db import metadata
from idu_api.common.db.entities.territories import territories_data

func: Callable

object_geometries_id_seq = Sequence("object_geometries_id_seq", schema="user_projects")

projects_object_geometries_data = Table(
    "object_geometries_data",
    metadata,
    Column("object_geometry_id", Integer, primary_key=True, server_default=object_geometries_id_seq.next_value()),
    Column(
        "territory_id",
        Integer,
        ForeignKey(territories_data.c.territory_id),
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
    Column("address", String(300)),
    Column("osm_id", String(20)),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Object geometries data:
- object_geometry_id int 
- territory_id foreign key int
- geometry geometry
- centre_point geometry point
"""
