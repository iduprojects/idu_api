from typing import Callable

from sqlalchemy import TIMESTAMP, Boolean, Column, ForeignKey, Integer, Sequence, String, Table, func

from idu_api.common.db import metadata

func: Callable

project_id_seq = Sequence("project_id_seq", schema="user_projects")

projects_data = Table(
    "projects_data",
    metadata,
    Column("project_id", Integer, primary_key=True, server_default=project_id_seq.next_value()),
    Column("user_id", Integer, nullable=False, unique=False),
    Column("name", String(200), nullable=False, unique=False),
    Column(
        "project_territory_id",
        Integer,
        ForeignKey("user_projects.projects_territory_data.project_territory_id"),
        nullable=False,
    ),
    Column("description", String(600), nullable=True, unique=False),
    Column("public", Boolean, nullable=False, unique=False),
    Column("image_url", String(200), nullable=True, unique=False),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), nullable=False),
    schema="user_projects",
)

"""
Projects:
- project_id int 
- user_id int
- name str
- project_territory_id int
- description str
- public bool
- image_url str
"""
