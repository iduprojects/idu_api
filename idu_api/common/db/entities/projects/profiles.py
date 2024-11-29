"""Profiles data table is defined here."""

from typing import Callable

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.functional_zones import functional_zone_types_dict
from idu_api.common.db.entities.projects.scenarios import scenarios_data

func: Callable

profiles_data_id_seq = Sequence("profiles_data_id_seq")

profiles_data = Table(
    "profiles_data",
    metadata,
    Column("profile_id", Integer, primary_key=True, server_default=profiles_data_id_seq.next_value()),
    Column(
        "scenario_id",
        Integer,
        ForeignKey(scenarios_data.c.scenario_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "functional_zone_type_id",
        Integer,
        ForeignKey(functional_zone_types_dict.c.functional_zone_type_id),
        nullable=False,
    ),
    Column("name", String(200), nullable=True),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry"),
        nullable=False,
    ),
    Column("year", Integer, nullable=True),
    Column("source", String(200), nullable=True),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Profiles data:
- profile_id int
- scenario_id foreign key int
- functional_zone_type_id foreign key int
- geometry geometry
- year int 
- source str
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""
