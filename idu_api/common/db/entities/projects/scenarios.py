"""Scenarios data table is defined here."""

from typing import Callable

from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.enums import ScenarioPhase
from idu_api.common.db.entities.functional_zones import functional_zone_types_dict
from idu_api.common.db.entities.projects.projects import projects_data

func: Callable

ScenarioPhaseEnum = Enum(ScenarioPhase, name="scenario_phase")


scenarios_data_id_seq = Sequence("scenarios_data_id_seq", schema="user_projects")

scenarios_data = Table(
    "scenarios_data",
    metadata,
    Column("scenario_id", Integer, primary_key=True, server_default=scenarios_data_id_seq.next_value()),
    Column(
        "parent_id", Integer, ForeignKey("user_projects.scenarios_data.scenario_id", ondelete="CASCADE"), nullable=True
    ),
    Column("project_id", Integer, ForeignKey(projects_data.c.project_id, ondelete="CASCADE"), nullable=False),
    Column(
        "functional_zone_type_id",
        Integer,
        ForeignKey(functional_zone_types_dict.c.functional_zone_type_id),
        nullable=True,
    ),
    Column("name", String(200), nullable=False),
    Column("is_based", Boolean, nullable=False),
    Column("phase", ScenarioPhaseEnum, nullable=True),
    Column("phase_percentage", Float(3), nullable=True),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Scenarios data:
- scenario_id int 
- project_id foreign key int
- functional_zone_type_id foreign key int
- name str
- is_based bool
- phase enum
- phase_percentage float
- properties jsonb
- created_at timestamp
- updated_at timestamp
"""
