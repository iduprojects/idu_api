from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, String, Table

from idu_api.common.db import metadata

profiles_data = Table(
    "profiles_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id"),
        nullable=False,
    ),
    Column("profile_type_id", Integer, nullable=False),
    Column("name", String(200), nullable=False, unique=False),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry"),
        nullable=False,
    ),
    schema="user_projects",
)

"""
Profiles data:
- scenario_id foreign key int
- profile_type_id int
- name str 
- geometry geometry
"""
