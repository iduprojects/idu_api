"""
Object geometries data table is defined here
"""

from typing import Callable

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, func

from idu_api.common.db import metadata

func: Callable

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
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Object geometries:
- object_geometry_id int 
- territory_id foreign key int
- geometry geometry 
- centre_point geometry point
- address string(300)
"""
