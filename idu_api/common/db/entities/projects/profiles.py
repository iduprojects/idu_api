from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, String, Table

from idu_api.common.db import metadata
from idu_api.common.db.entities.projects.target_profiles import target_profiles_dict

profiles_data = Table(
    "profiles_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id"),
        nullable=False,
    ),
    Column("target_profile_id", Integer, ForeignKey(target_profiles_dict.c.target_profile_id), nullable=False),
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
- target_profile_id foreign key int
- name str 
- geometry geometry
"""
