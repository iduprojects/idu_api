from geoalchemy2 import Geometry
from sqlalchemy.sql.expression import false
from sqlalchemy.sql.schema import Column, ForeignKey, PrimaryKeyConstraint, Table
from sqlalchemy.sql.sqltypes import Boolean, Integer

from idu_api.common.db import metadata
from idu_api.common.db.entities.buffers import buffer_types_dict
from idu_api.common.db.entities.projects.urban_objects import urban_objects_data

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
    schema="user_projects",
)

"""
Buffers Data:
- buffer_type_id int (Foreign Key to buffer_types_dict)
- urban_object_id int (Foreign Key to urban_objects_data, ondelete CASCADE)
- geometry geometry
- is_custom bool
"""
