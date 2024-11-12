"""Tables which represent agglomerations are defined here.

Current list is:
- aglomeration_types_dict
- aglomeration_data
"""

from typing import Callable

from geoalchemy2 import Geometry
from sqlalchemy import (
    TIMESTAMP,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    Sequence,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata

func: Callable

aglomeration_types_dict_id_seq = Sequence("aglomeration_types_dict_id_seq")

aglomeration_types_dict = Table(
    "aglomeration_types_dict",
    metadata,
    Column(
        "aglomeration_type_id",
        Integer,
        primary_key=True,
        server_default=aglomeration_types_dict_id_seq.next_value(),
        nullable=False,
    ),
    Column("name", String(200), nullable=False),
    PrimaryKeyConstraint("aglomeration_type_id", name="aglomeration_types_dict_pk"),
    UniqueConstraint("name", name="aglomeration_types_dict_name_key"),
)

"""
Agglomeration type:
- aglomeration_type_id int
- name string(200)
"""

aglomeration_data_id_seq = Sequence("aglomeration_data_id_seq")

aglomeration_data = Table(
    "aglomeration_data",
    metadata,
    Column(
        "aglomeration_id",
        Integer,
        primary_key=True,
        server_default=aglomeration_data_id_seq.next_value(),
        nullable=False,
    ),
    Column("aglomeration_type_id", ForeignKey("aglomeration_types_dict.aglomeration_type_id"), nullable=False),
    Column("parent_id", ForeignKey("aglomeration_data.aglomeration_id"), nullable=True),
    Column("name", String(200), nullable=False),
    Column("geometry", Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False)),
    Column("level", Integer, nullable=False),
    Column("properties", JSONB(astext_type=String()), nullable=False, server_default=text("'{}'::jsonb")),
    Column(
        "centre_point",
        Geometry(
            geometry_type="POINT", spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False
        ),
    ),
    Column("admin_center", Integer, nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    ForeignKeyConstraint(
        ["parent_id"], ["aglomeration_data.aglomeration_id"], name="aglomeration_data_fk_parent_id__aglomeration_data"
    ),
    ForeignKeyConstraint(
        ["aglomeration_type_id"],
        ["aglomeration_types_dict.aglomeration_type_id"],
        name="aglomeration_data_fk_aglomeration_type_id__aglomeration_types_dict",
    ),
    PrimaryKeyConstraint("aglomeration_id", name="aglomeration_data_pk"),
)

"""
Agglomeration:
- aglomeration_id int 
- aglomeration_type_id foreign key int
- parent_id foreign key int
- name string(200)
- geometry geometry 
- level int
- properties jsonb
- centre_point geometry point
- admin_center int
- created_at timestamp
- updated_at timestamp
"""
