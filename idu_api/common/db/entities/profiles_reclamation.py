from sqlalchemy import Column, Float, ForeignKey, Integer, Sequence, Table, UniqueConstraint

from idu_api.common.db import metadata
from idu_api.common.db.entities.territories import territories_data
from idu_api.common.db.entities.urban_types_dicts import functional_zone_types_dict

profile_reclamation_id_seq = Sequence("profile_reclamation_id_seq")

profiles_reclamation_data = Table(
    "profiles_reclamation_data",
    metadata,
    Column("profile_reclamation_id", Integer, primary_key=True, server_default=profile_reclamation_id_seq.next_value()),
    Column(
        "source_profile_id", Integer, ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=False
    ),
    Column(
        "target_profile_id", Integer, ForeignKey(functional_zone_types_dict.c.functional_zone_type_id), nullable=False
    ),
    Column("territory_id", Integer, ForeignKey(territories_data.c.territory_id), nullable=True),
    Column("technical_price", Float, nullable=False),
    Column("technical_time", Float, nullable=False),
    Column("biological_price", Float, nullable=False),
    Column("biological_time", Float, nullable=False),
    UniqueConstraint("source_profile_id", "target_profile_id", "territory_id"),
)

"""
Profiles reclamation:
- profile_reclamation_id primary key int
- source_profile_id foreign key int
- target_profile_id foreign key int
- territory_id foreign key int
- technical_price float 
- technical_time float 
- biological_price float 
- biological_time float 
"""
