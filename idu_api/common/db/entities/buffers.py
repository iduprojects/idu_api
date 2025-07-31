"""Buffers data tables are defined here."""

from geoalchemy2.types import Geometry
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    false,
)

from idu_api.common.db import metadata
from idu_api.common.db.entities.physical_object_types import physical_object_types_dict
from idu_api.common.db.entities.service_types import service_types_dict
from idu_api.common.db.entities.urban_objects import urban_objects_data

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

default_buffer_values_dict_id_seq = Sequence("default_buffer_values_dict_id_seq")

default_buffer_values_dict = Table(
    "default_buffer_values_dict",
    metadata,
    Column(
        "default_buffer_value_id",
        Integer,
        primary_key=True,
        server_default=default_buffer_values_dict_id_seq.next_value(),
    ),
    Column(
        "buffer_type_id", Integer, ForeignKey(buffer_types_dict.c.buffer_type_id, ondelete="CASCADE"), nullable=False
    ),
    Column(
        "physical_object_type_id",
        Integer,
        ForeignKey(physical_object_types_dict.c.physical_object_type_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column(
        "service_type_id",
        Integer,
        ForeignKey(service_types_dict.c.service_type_id, ondelete="CASCADE"),
        nullable=True,
    ),
    Column("buffer_value", Float(precision=53), nullable=False),
    UniqueConstraint("buffer_type_id", "physical_object_type_id", "service_type_id"),
)

"""
Default Buffer Values Dictionary:
- default_buffer_value_id int (Primary Key)
- buffer_type_id int (Foreign Key to buffer_types_dict, ondelete CASCADE)
- physical_object_type_id int (Foreign Key to physical_object_types_dict, ondelete CASCADE)
- service_type_id int (Foreign Key to service_types_dict, ondelete CASCADE)
- buffer_value float
"""

buffers_data = Table(
    "buffers_data",
    metadata,
    Column("buffer_type_id", Integer, ForeignKey(buffer_types_dict.c.buffer_type_id), nullable=False),
    Column(
        "urban_object_id", Integer, ForeignKey(urban_objects_data.c.urban_object_id, ondelete="CASCADE"), nullable=False
    ),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("is_custom", Boolean(), server_default=false(), nullable=False),
    PrimaryKeyConstraint("buffer_type_id", "urban_object_id"),
)

"""
Buffers Data:
- buffer_type_id int (Foreign Key to buffer_types_dict)
- urban_object_id int (Foreign Key to urban_objects_data, ondelete CASCADE)
- geometry geometry
- is_custom bool
"""
