from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, String, Table

from idu_api.common.db import metadata

projects_functional_zones_data = Table(
    "functional_zones_data",
    metadata,
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "functional_zone_type_id",
        Integer,
        ForeignKey("functional_zone_types_dict.functional_zone_type_id"),
        nullable=False,
    ),
    Column("name", String(200), nullable=False, unique=False),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry"),
        nullable=False,
    ),
    schema="user_projects",
)

"""
Functional zones data:
- scenario_id foreign key int
- functional_zone_type_id foreign key int
- name str
- geometry geometry
"""
