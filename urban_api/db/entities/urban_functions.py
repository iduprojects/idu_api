"""
Urban functions dict table is defined here
"""

from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, Table

from urban_api.db import metadata

urban_functions_dict_id_seq = Sequence("urban_functions_dict_id_seq")

urban_functions_dict = Table(
    "urban_functions_dict",
    metadata,
    Column("urban_function_id", Integer, primary_key=True, server_default=urban_functions_dict_id_seq.next_value()),
    Column("parent_urban_function_id", ForeignKey("urban_functions_dict.urban_function_id")),
    Column("name", String(200), nullable=False, unique=True),
    Column("level", Integer, nullable=False),
    Column("list_label", String(20), nullable=False, unique=True),
    Column("code", String(50), nullable=False),
)

"""
Urban functions dict:
- urban_function_id int 
- parent_urban_function_id foreign key int
- name string(200)
- level int
- list_label string(20)
- code string(50)
"""
