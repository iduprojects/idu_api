"""Hexagons schemas are defined here."""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import HexagonDTO
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel
from idu_api.urban_api.schemas.short_models import ShortTerritory


class Hexagon(BaseModel):
    """Hexagon with all its attributes."""

    hexagon_id: int = Field(..., description="hexagon id", examples=[1])
    territory: ShortTerritory
    geometry: Geometry
    centre_point: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="hexagon properties",
        examples=[{"attribute_name": "attribute_value"}],
    )

    @classmethod
    def from_dto(cls, dto: HexagonDTO) -> "Hexagon":
        """Construct from DTO"""

        return cls(
            hexagon_id=dto.hexagon_id,
            territory=ShortTerritory(
                id=dto.territory_id,
                name=dto.territory_name,
            ),
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
            properties=dto.properties,
        )


class HexagonPost(GeometryValidationModel):
    """Hexagon schema for POST requests."""

    geometry: Geometry
    centre_point: Geometry
    properties: dict[str, Any] = Field(
        default_factory=dict,
        description="hexagon properties",
        examples=[{"attribute_name": "attribute_value"}],
    )
