from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table, Text, text
from sqlalchemy.dialects.postgresql import JSONB

from idu_api.common.db import metadata
from idu_api.common.db.entities.projects.projects import projects_data
from idu_api.common.db.entities.urban_types_dicts import functional_zone_types_dict

scenarios_data_id_seq = Sequence("scenarios_data_id_seq", schema="user_projects")

scenarios_data = Table(
    "scenarios_data",
    metadata,
    Column("scenario_id", Integer, primary_key=True, server_default=scenarios_data_id_seq.next_value()),
    Column("project_id", Integer, ForeignKey(projects_data.c.project_id, ondelete="CASCADE"), nullable=False),
    Column(
        "target_profile_id", Integer, ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=True
    ),
    Column("name", String(200), nullable=False, unique=False),
    Column("properties", JSONB(astext_type=Text()), nullable=False, server_default=text("'{}'::jsonb")),
    schema="user_projects",
)

"""
Scenarios data:
- scenario_id int 
- project_id foreign key int
- target_profile_id foreign key int
- name str
- properties jsonb
"""
