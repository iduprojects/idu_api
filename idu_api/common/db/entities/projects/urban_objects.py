from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table

from idu_api.common.db import metadata

urban_objects_id_seq = Sequence("urban_objects_id_seq", schema="user_projects")

projects_urban_objects_data = Table(
    "urban_objects_data",
    metadata,
    Column("urban_object_id", Integer, primary_key=True, server_default=urban_objects_id_seq.next_value()),
    Column(
        "object_geometry_id",
        Integer,
        ForeignKey("user_projects.object_geometries_data.object_geometry_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "physical_object_id",
        Integer,
        ForeignKey("user_projects.physical_objects_data.physical_object_id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "service_id",
        Integer,
        ForeignKey("user_projects.services_data.service_id", ondelete="CASCADE"),
        nullable=False
    ),
    Column(
        "scenario_id",
        Integer,
        ForeignKey("user_projects.scenarios_data.scenario_id", ondelete="CASCADE"),
        nullable=False
    ),
    schema="user_projects",
)

"""
Urban objects data:
- urban_object_id int 
- object_geometry_id foreign key int
- physical_object_id foreign key int
- service_id foreign key int
- scenario_id foreign key int
"""
