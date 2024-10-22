from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class PhysicalObjectTypeDTO:
    """Physical object type with all its attributes."""

    physical_object_type_id: int
    name: str
    physical_object_function_id: int | None
    physical_object_function_name: str | None


@dataclass(frozen=True)
class PhysicalObjectFunctionDTO:
    """Physical object function with all its attributes."""

    physical_object_function_id: int
    parent_id: int | None
    parent_name: str | None
    name: str
    level: int
    list_label: str
    code: str


@dataclass(frozen=True)
class PhysicalObjectTypesHierarchyDTO:
    """Physical object hierarchy DTO."""

    physical_object_function_id: int
    parent_id: int | None
    name: str
    level: int
    list_label: str
    code: str
    children: list[Self | PhysicalObjectTypeDTO]
