"""Territories data table is defined here."""

from typing import Callable

from geoalchemy2.types import Geometry
from sqlalchemy import TIMESTAMP, Boolean, Column, ForeignKey, Integer, Sequence, String, Table, Text, false, func, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

func: Callable

territory_types_dict_id_seq = Sequence("territory_types_dict_id_seq")

territory_types_dict = Table(
    "territory_types_dict",
    metadata,
    Column("territory_type_id", Integer, primary_key=True, server_default=territory_types_dict_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=True),
)

"""
Territory types:
- territory_type_id int 
- name string(200)
"""

target_city_types_dict_id_seq = Sequence("target_city_types_dict_id_seq")

target_city_types_dict = Table(
    "target_city_types_dict",
    metadata,
    Column("target_city_type_id", Integer, primary_key=True, server_default=target_city_types_dict_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=True),
    Column("description", String(2048), nullable=False),
)

"""
Target city types:
- target_city_type_id int 
- name string(200)
- description string(200)
"""

territories_data_id_seq = Sequence("territories_data_id_seq")

territories_data = Table(
    "territories_data",
    metadata,
    Column("territory_id", Integer, primary_key=True, server_default=territories_data_id_seq.next_value()),
    Column("territory_type_id", ForeignKey(territory_types_dict.c.territory_type_id), nullable=False),
    Column("parent_id", ForeignKey("territories_data.territory_id"), nullable=True),
    Column("name", String(200), nullable=False),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("level", Integer, nullable=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "centre_point",
        Geometry("POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
    Column("admin_center_id", ForeignKey("territories_data.territory_id"), nullable=True),
    Column("target_city_type_id", ForeignKey(target_city_types_dict.c.target_city_type_id), nullable=True),
    Column("okato_code", String(20)),
    Column("oktmo_code", String(20)),
    Column("is_city", Boolean, nullable=False, server_default=false()),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
)

"""
Territories:
- territory_id int 
- territory_type_id foreign key int
- parent_id foreign key int
- name string(200)
- geometry geometry 
- level int
- properties jsonb
- centre_point geometry point
- admin_center_id foreign key int
- target_city_type_id foreign key int
- okato_code string(20)
- oktmo_code string(20)
- is_city bool
- created_at timestamp
- updated_at timestamp
"""
