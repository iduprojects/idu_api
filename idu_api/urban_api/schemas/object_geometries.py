from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import ObjectGeometryDTO
from idu_api.urban_api.schemas.geometries import Geometry, GeometryValidationModel


class ObjectGeometries(BaseModel):
    object_geometry_id: int = Field(..., examples=[1])
    territory_id: int = Field(..., examples=[1])
    address: str | None = Field(None, description="Physical object address", examples=["--"])
    geometry: Geometry
    centre_point: Geometry

    @classmethod
    def from_dto(cls, dto: ObjectGeometryDTO) -> "ObjectGeometries":
        """
        Construct from DTO.
        """
        return cls(
            object_geometry_id=dto.object_geometry_id,
            territory_id=dto.territory_id,
            address=dto.address,
            geometry=Geometry.from_shapely_geometry(dto.geometry),
            centre_point=Geometry.from_shapely_geometry(dto.centre_point),
        )


class ObjectGeometriesPost(GeometryValidationModel):
    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry | None = Field(None, description="Centre coordinates")
    address: str | None = Field(None, description="Physical object address", examples=["--"])


class ObjectGeometriesPut(GeometryValidationModel):
    territory_id: int = Field(..., examples=[1])
    geometry: Geometry
    centre_point: Geometry
    address: str | None = Field(..., description="Physical object address", examples=["--"])


class ObjectGeometriesPatch(GeometryValidationModel):
    territory_id: int | None = Field(None, examples=[1])
    geometry: Geometry | None = Field(None, description="Object geometry")
    centre_point: Geometry | None = Field(None, description="Centre coordinates")
    address: str | None = Field(None, description="Physical object address", examples=["--"])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """
        Ensure the request body is not empty.
        """
        if not values:
            raise ValueError("request body cannot be empty")
        return values
