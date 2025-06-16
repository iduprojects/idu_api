"""Projects data table is defined here."""

from typing import Callable

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
    Text,
    false,
    func,
    null,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.territories import territories_data

func: Callable

project_id_seq = Sequence("project_id_seq", schema="user_projects")

projects_data = Table(
    "projects_data",
    metadata,
    Column("project_id", Integer, primary_key=True, server_default=project_id_seq.next_value()),
    Column("user_id", String(200), nullable=False),
    Column("name", String(200), nullable=False),
    Column(
        "territory_id",
        Integer,
        ForeignKey(territories_data.c.territory_id),
        nullable=False,
    ),
    Column("description", String(4096), nullable=True),
    Column("public", Boolean, nullable=False),
    Column("is_regional", Boolean, nullable=False),
    Column("is_city", Boolean, nullable=False, server_default=false()),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Project data:
- project_id int
- user_id string
- name string
- territory_id foreign key int
- description string(4086)
- public bool
- is_regional bool
- is_city bool
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""

projects_phases_data = Table(
    "projects_phases_data",
    metadata,
    Column(
        "project_id",
        Integer,
        ForeignKey(projects_data.c.project_id),
        primary_key=True,
        nullable=False,
    ),
    Column("actual_start_date", Date(), server_default=null(), nullable=True),
    Column("actual_end_date", Date(), server_default=null(), nullable=True),
    Column("planned_start_date", Date(), server_default=null(), nullable=True),
    Column("planned_end_date", Date(), server_default=null(), nullable=True),
    Column("investment", Float(precision=53), server_default=text("0"), nullable=False),
    Column("pre_design", Float(precision=53), server_default=text("0"), nullable=False),
    Column("design", Float(precision=53), server_default=text("0"), nullable=False),
    Column("construction", Float(precision=53), server_default=text("0"), nullable=False),
    Column("operation", Float(precision=53), server_default=text("0"), nullable=False),
    Column("decommission", Float(precision=53), server_default=text("0"), nullable=False),
    schema="user_projects",
)

"""
Project data:
- project_id foreign key int
- actual_start_date date
- actual_end_date date
- planned_start_date date
- planned_end_date date
- investment float
- pre_design float
- design float
- construction float
- operation float
- decommission float
"""
