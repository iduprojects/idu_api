from sqlalchemy import Column, ForeignKey, Integer, PrimaryKeyConstraint, Sequence, String, Table

from idu_api.common.db import metadata

indicators_groups_dict_id_seq = Sequence("indicators_groups_dict_id_seq")

indicators_groups_dict = Table(
    "indicators_groups_dict",
    metadata,
    Column("indicators_group_id", Integer, primary_key=True, server_default=indicators_groups_dict_id_seq.next_value()),
    Column("name", String(length=100), nullable=False, unique=True),
)

"""
Indicators Groups Dictionary:
- indicators_group_id int (Primary Key)
- name string(100) (Unique)
"""

indicators_groups_data = Table(
    "indicators_groups_data",
    metadata,
    Column("indicator_id", Integer, ForeignKey("indicators_dict.indicator_id", ondelete="CASCADE"), nullable=False),
    Column(
        "indicators_group_id",
        Integer,
        ForeignKey("indicators_groups_dict.indicators_group_id", ondelete="CASCADE"),
        nullable=False,
    ),
    PrimaryKeyConstraint("indicator_id", "indicators_group_id"),
)

"""
Indicators Groups Data:
- indicator_id int (Foreign Key to indicators_dict, ondelete CASCADE)
- indicators_group_id int (Foreign Key to indicators_groups_dict, ondelete CASCADE)
"""
