"""
Urban objects data table is defined here
"""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table, UniqueConstraint

from idu_api.common.db import metadata

urban_objects_data_id_seq = Sequence("urban_objects_data_id_seq")

urban_objects_data = Table(
    "urban_objects_data",
    metadata,
    Column("urban_object_id", Integer, primary_key=True, server_default=urban_objects_data_id_seq.next_value()),
    Column("physical_object_id", ForeignKey("physical_objects_data.physical_object_id"), nullable=False),
    Column("object_geometry_id", ForeignKey("object_geometries_data.object_geometry_id"), nullable=False),
    Column("service_id", ForeignKey("services_data.service_id")),
    UniqueConstraint("physical_object_id", "object_geometry_id"),
)

"""
Urban objects:
- urban_object_id int 
- territory_id foreign key int
- object_geometry_id foreign key int
- service_id foreign key int
"""
