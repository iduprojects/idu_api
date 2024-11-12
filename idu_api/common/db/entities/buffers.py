"""Buffers data tables are defined here."""

from sqlalchemy import Column, ForeignKey, Integer, PrimaryKeyConstraint, Sequence, String, Table

from idu_api.common.db import metadata

buffer_types_dict_id_seq = Sequence("buffer_types_dict_id_seq")

buffer_types_dict = Table(
    "buffer_types_dict",
    metadata,
    Column("buffer_type_id", Integer, primary_key=True, server_default=buffer_types_dict_id_seq.next_value()),
    Column("name", String(length=100), nullable=False, unique=True),
)

"""
Buffer Types Dictionary:
- buffer_type_id int (Primary Key)
- name string(100) (Unique)
"""

buffers_data = Table(
    "buffers_data",
    metadata,
    Column("buffer_type_id", Integer, ForeignKey("buffer_types_dict.buffer_type_id"), nullable=False),
    Column(
        "urban_object_id", Integer, ForeignKey("urban_objects_data.urban_object_id", ondelete="CASCADE"), nullable=False
    ),
    Column("buffer_geometry_id", Integer, ForeignKey("object_geometries_data.object_geometry_id"), nullable=False),
    PrimaryKeyConstraint("buffer_type_id", "urban_object_id", "buffer_geometry_id"),
)

"""
Buffers Data:
- buffer_type_id int (Foreign Key to buffer_types_dict)
- urban_object_id int (Foreign Key to urban_objects_data, ondelete CASCADE)
- buffer_geometry_id int (Foreign Key to object_geometries_data)
"""
