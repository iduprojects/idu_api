"""Projects urban objects data table is defined here."""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table

from idu_api.common.db import metadata
from idu_api.common.db.entities.object_geometries import object_geometries_data
from idu_api.common.db.entities.physical_objects import physical_objects_data
from idu_api.common.db.entities.projects.object_geometries import projects_object_geometries_data
from idu_api.common.db.entities.projects.physical_objects import projects_physical_objects_data
from idu_api.common.db.entities.projects.scenarios import scenarios_data
from idu_api.common.db.entities.projects.services import projects_services_data
from idu_api.common.db.entities.services import services_data
from idu_api.common.db.entities.urban_objects import urban_objects_data

urban_objects_id_seq = Sequence("urban_objects_id_seq", schema="user_projects")

projects_urban_objects_data = Table(
    "urban_objects_data",
    metadata,
    Column("urban_object_id", Integer, primary_key=True, server_default=urban_objects_id_seq.next_value()),
    Column(
        "scenario_id",
        Integer,
        ForeignKey(scenarios_data.c.scenario_id, ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "public_urban_object_id",
        Integer,
        ForeignKey(urban_objects_data.c.urban_object_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column(
        "object_geometry_id",
        Integer,
        ForeignKey(projects_object_geometries_data.c.object_geometry_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column(
        "physical_object_id",
        Integer,
        ForeignKey(projects_physical_objects_data.c.physical_object_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column("service_id", Integer, ForeignKey(projects_services_data.c.service_id, ondelete="SET NULL"), nullable=True),
    Column(
        "public_object_geometry_id",
        Integer,
        ForeignKey(object_geometries_data.c.object_geometry_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column(
        "public_physical_object_id",
        Integer,
        ForeignKey(physical_objects_data.c.physical_object_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column("public_service_id", Integer, ForeignKey(services_data.c.service_id, ondelete="SET NULL"), nullable=True),
    schema="user_projects",
)

"""
Urban objects data:
- urban_object_id int
- scenario_id foreign key int
- public_urban_object_id foreign key int 
- object_geometry_id foreign key int
- physical_object_id foreign key int
- service_id foreign key int
- public_object_geometry_id foreign key int
- public_physical_object_id foreign key int
- public_service_id foreign key int
"""
