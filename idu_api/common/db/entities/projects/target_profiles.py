from sqlalchemy import Column, Integer, Sequence, String, Table

from idu_api.common.db import metadata

target_profile_id_seq = Sequence("target_profile_id_seq", schema="user_projects")

target_profiles_dict = Table(
    "target_profiles_dict",
    metadata,
    Column("target_profile_id", Integer, primary_key=True, server_default=target_profile_id_seq.next_value()),
    Column("name", String(200), nullable=False, unique=False),
    schema="user_projects",
)

"""
Target profiles dict:
- target_profile_id int pk
- name str 
"""
