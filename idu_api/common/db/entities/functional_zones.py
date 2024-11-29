"""Tables which represent functional zones are defined here.

Current list is:
- functional_zone_types_dict
- functional_zones_data
"""

from typing import Callable

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer, Sequence, String, Table, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.territories import territories_data

func: Callable

functional_zone_types_dict_id_seq = Sequence("functional_zone_types_dict_id_seq")

functional_zone_types_dict = Table(
    "functional_zone_types_dict",
    metadata,
    Column(
        "functional_zone_type_id",
        Integer,
        primary_key=True,
        server_default=functional_zone_types_dict_id_seq.next_value(),
    ),
    Column("name", String(200), nullable=False, unique=True),
    Column("zone_nickname", String(200)),
    Column("description", Text),
)

"""
Functional zone types:
- functional_zone_type_id int 
- name string(100)
- zone_nickname string(100)
- description text
"""

functional_zones_data_id_seq = Sequence("functional_zones_data_id_seq")

functional_zones_data = Table(
    "functional_zones_data",
    metadata,
    Column("functional_zone_id", Integer, primary_key=True, server_default=functional_zones_data_id_seq.next_value()),
    Column("territory_id", ForeignKey(territories_data.c.territory_id), nullable=False),
    Column("functional_zone_type_id", ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=False),
    Column("name", String(200), nullable=True),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("year", Integer, nullable=True),
    Column("source", String(200), nullable=True),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Functional zones:
- functional_zone_id int 
- territory_id foreign key int
- functional_zone_type_id foreign key int
- geometry geometry 
- year int 
- source str
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""
