"""
Functional zones data table is defined here
"""

from geoalchemy2.types import Geometry
from sqlalchemy import Column, ForeignKey, Integer, Sequence, Table

from idu_api.common.db import metadata

functional_zones_data_id_seq = Sequence("functional_zones_data_id_seq")

functional_zones_data = Table(
    "functional_zones_data",
    metadata,
    Column("functional_zone_id", Integer, primary_key=True, server_default=functional_zones_data_id_seq.next_value()),
    Column("territory_id", ForeignKey("territories_data.territory_id"), nullable=False),
    Column("functional_zone_type_id", ForeignKey("functional_zone_types_dict.functional_zone_type_id"), nullable=False),
    Column(
        "geometry",
        Geometry(spatial_index=False, from_text="ST_GeomFromEWKT", name="geometry", nullable=False),
        nullable=False,
    ),
)

"""
Functional zones:
- functional_zone_id int 
- territory_id foreign key int
- functional_zone_type_id foreign key int
- geometry geometry 
"""
