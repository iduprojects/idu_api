"""Projects territories data table is defined here."""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.projects.projects import projects_data

project_territory_id_seq = Sequence("project_territory_id_seq", schema="user_projects")

projects_territory_data = Table(
    "projects_territory_data",
    metadata,
    Column("project_territory_id", Integer, primary_key=True, server_default=project_territory_id_seq.next_value()),
    Column(
        "project_id",
        Integer,
        ForeignKey(projects_data.c.project_id, ondelete="CASCADE"),
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
Project territory data:
- project_territory_id int 
- project_id foreign key int
- geometry geometry
- centre_point geometry point
- properties jsonb
"""
