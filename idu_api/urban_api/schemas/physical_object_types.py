"""Physical object type schemas are defined here."""

from typing import Self

from pydantic import BaseModel, Field, model_validator

from idu_api.urban_api.dto import (
    PhysicalObjectFunctionDTO,
    PhysicalObjectTypeDTO,
    PhysicalObjectTypesHierarchyDTO,
)
from idu_api.urban_api.schemas.short_models import PhysicalObjectFunctionBasic


class PhysicalObjectsTypes(BaseModel):
    """Physical object type with all its attributes."""

    physical_object_type_id: int = Field(..., description="physical object type identifier", examples=[1])
    name: str = Field(..., description="physical object type unit name", examples=["Здание"])
    physical_object_function: PhysicalObjectFunctionBasic | None

    @classmethod
    def from_dto(cls, dto: PhysicalObjectTypeDTO) -> "PhysicalObjectsTypes":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_type_id=dto.physical_object_type_id,
            name=dto.name,
            physical_object_function=(
                PhysicalObjectFunctionBasic(
                    id=dto.physical_object_function_id,
                    name=dto.physical_object_function_name,
                )
                if dto.physical_object_function_id is not None
                else None
            ),
        )


class PhysicalObjectsTypesPost(BaseModel):
    """Schema of physical object type for POST request."""

    name: str = Field(..., description="physical object type unit name", examples=["Здание"])
    physical_object_function_id: int = Field(..., description="function identifier", examples=[1])


class PhysicalObjectsTypesPatch(BaseModel):
    """Schema of physical object type for PATCH request."""

    name: str | None = Field(None, description="physical object type unit name", examples=["Здание"])
    physical_object_function_id: int | None = Field(None, description="function identifier", examples=[1])

    @model_validator(mode="before")
    @classmethod
    def check_empty_request(cls, values):
        """Ensure the request body is not empty."""
        if not values:
            raise ValueError("request body cannot be empty")
        return values


class PhysicalObjectFunction(BaseModel):
    """Physical object function with all its attributes."""

    physical_object_function_id: int = Field(..., examples=[1])
    parent_physical_object_function: PhysicalObjectFunctionBasic | None
    name: str = Field(..., description="physical object function unit name", examples=["--"])
    level: int = Field(..., description="number of urban functions above in a tree + [1]", examples=[1])
    list_label: str = Field(..., description="physical object function list label", examples=["1.1.1"])
    code: str = Field(..., description="physical object function code", examples=["1"])

    @classmethod
    def from_dto(cls, dto: PhysicalObjectFunctionDTO) -> "PhysicalObjectFunction":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_function_id=dto.physical_object_function_id,
            parent_physical_object_function=(
                PhysicalObjectFunctionBasic(id=dto.parent_id, name=dto.parent_name)
                if dto.parent_id is not None
                else None
            ),
            name=dto.name,
            level=dto.level,
            list_label=dto.list_label,
            code=dto.code,
        )


class PhysicalObjectFunctionPost(BaseModel):
    """Schema of physical object function for POST request."""

    name: str = Field(..., description="physical object function unit name", examples=["--"])
    parent_id: int | None = Field(None, description="physical object function parent id, if set", examples=[1])
    code: str = Field(..., description="physical object function code", examples=["1"])


class PhysicalObjectFunctionPut(BaseModel):
    """Schema of physical object function for PUT request."""

    name: str = Field(..., description="physical object function unit name", examples=["--"])
    parent_id: int | None = Field(..., description="physical object function parent id, if set", examples=[1])
    code: str = Field(..., description="physical object function code", examples=["1"])


class PhysicalObjectFunctionPatch(BaseModel):
    """Schema of physical object function for PATCH request."""

    name: str | None = Field(..., description="physical object function unit name", examples=["--"])
    parent_id: int | None = Field(None, description="physical object function parent id, if set", examples=[1])
    code: str | None = Field(..., description="physical object function code", examples=["1"])


class PhysicalObjectsTypesHierarchy(BaseModel):
    physical_object_function_id: int = Field(..., examples=[1])
    parent_id: int | None = Field(
        ..., description="parent physical object function identifier (null if it is top-level function)", examples=[1]
    )
    name: str = Field(..., description="physical object function unit name", examples=["--"])
    level: int = Field(..., description="number of physical object functions above in a tree + [1]", examples=[1])
    list_label: str = Field(..., description="physical object function list label", examples=["1.1.1"])
    code: str = Field(..., description="physical object function code", examples=["1"])
    children: list[Self | PhysicalObjectsTypes]

    @classmethod
    def from_dto(cls, dto: PhysicalObjectTypesHierarchyDTO) -> "PhysicalObjectsTypesHierarchy":
        """
        Construct from DTO.
        """
        return cls(
            physical_object_function_id=dto.physical_object_function_id,
            parent_id=dto.parent_id,
            name=dto.name,
            level=dto.level,
            list_label=dto.list_label,
            code=dto.code,
            children=[
                (
                    PhysicalObjectsTypesHierarchy.from_dto(child)
                    if isinstance(child, PhysicalObjectTypesHierarchyDTO)
                    else PhysicalObjectsTypes.from_dto(child)
                )
                for child in dto.children
            ],
        )
