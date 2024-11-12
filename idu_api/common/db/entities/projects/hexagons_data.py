"""Hexagons data table is defined here."""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.territories import territories_data

hexagons_data_id_seq = Sequence("hexagons_data_id_seq")

hexagons_data = Table(
    "hexagons_data",
    metadata,
    Column("hexagon_id", Integer, primary_key=True, server_default=hexagons_data_id_seq.next_value()),
    Column(
        "territory_id",
        Integer,
        ForeignKey(territories_data.c.territory_id, ondelete="CASCADE"),
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
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    schema="user_projects",
)

"""
Hexagons data:
- hexagon_id int
- territory_id foreign key int
- geometry geometry
- centre_point geometry
- properties jsonb
"""
