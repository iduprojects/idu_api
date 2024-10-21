"""Physical object functions dict table is defined here."""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table

from idu_api.common.db import metadata

physical_object_functions_dict_id_seq = Sequence("physical_object_functions_dict_id_seq")

physical_object_functions_dict = Table(
    "physical_object_functions_dict",
    metadata,
    Column(
        "physical_object_function_id",
        Integer,
        primary_key=True,
        server_default=physical_object_functions_dict_id_seq.next_value(),
    ),
    Column("parent_id", Integer, ForeignKey("physical_object_functions_dict.physical_object_function_id")),
    Column("name", String(200), nullable=False, unique=True),
    Column("level", Integer, nullable=False),
    Column("list_label", String(20), nullable=False, unique=True),
    Column("code", String(50), nullable=False),
)

"""
Physical object functions dict:
- physical_object_function_id int 
- parent_id foreign key int
- name string(200)
- level int
- list_label string(20)
- code string(50)
"""
