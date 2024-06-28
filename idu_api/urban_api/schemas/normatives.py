"""Normatives schemas are defined here."""

from enum import Enum

from pydantic import BaseModel, model_validator

from .service_types import ServiceTypeBasic, UrbanFunctionBasic


class NormativeType(Enum):
    SELF = "self"
    PARENT = "parent"
    GLOBAL = "global"


class Normative(BaseModel):
    """Normative response model for a given territory"""

    service_type: ServiceTypeBasic | None = None
    urban_function: UrbanFunctionBasic | None = None
    radius_availability_meters: int | None = None
    time_availability_minutes: int | None = None
    services_per_1000_normative: int | None = None
    services_capacity_per_1000_normative: int | None = None
    normative_type: NormativeType
    is_regulated: bool

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type is None and self.urban_function is None:
            raise ValueError("service_type and urban_function cannot both be unset")
        if self.service_type is not None and self.urban_function is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is None and self.time_availability_minutes is None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be unset")
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is None and self.services_capacity_per_1000_normative is None:
            raise ValueError(
                "services_per_1000_normative and services_capacity_per_1000_normative cannot both be unset"
            )
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self


class NormativePost(BaseModel):
    """Normative post/put model.

    Either service_type_id or urban_function_id should be set,
    same with radius_availability_meters and time_availability_minutes and
    services_per_1000_normative andservices_capacity_per_1000_normative.
    """

    service_type_id: int | None = None
    urban_function_id: int | None = None
    radius_availability_meters: int | None = None
    time_availability_minutes: int | None = None
    services_per_1000_normative: int | None = None
    services_capacity_per_1000_normative: int | None = None
    is_regulated: bool

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is None and self.urban_function_id is None:
            raise ValueError("service_type and urban_function cannot both be unset")
        if self.service_type_id is not None and self.urban_function_id is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is None and self.time_availability_minutes is None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be unset")
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is None and self.services_capacity_per_1000_normative is None:
            raise ValueError(
                "services_per_1000_normative and services_capacity_per_1000_normative cannot both be unset"
            )
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self


class NormativePatch(BaseModel):
    """Normative patch model.
    Either service_type_id or urban_function_id can be set,
    same with radius_availability_meters and time_availability_minutes and
    services_per_1000_normative andservices_capacity_per_1000_normative.
    """

    service_type_id: int | None = None
    urban_function_id: int | None = None
    radius_availability_meters: int | None = None
    time_availability_minutes: int | None = None
    services_per_1000_normative: int | None = None
    services_capacity_per_1000_normative: int | None = None
    is_regulated: bool | None = None

    @model_validator(mode="after")
    def validate_service_type_or_urban_function(self):
        if self.service_type_id is not None and self.urban_function_id is not None:
            raise ValueError("service_type and urban_function cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_radius_or_time_availability(self):
        if self.radius_availability_meters is not None and self.time_availability_minutes is not None:
            raise ValueError("radius_availability_meters and time_availability_minutes cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_services_or_capacity_normative(self):
        if self.services_per_1000_normative is not None and self.services_capacity_per_1000_normative is not None:
            raise ValueError("services_per_1000_normative and services_capacity_per_1000_normative cannot both be set")
        return self

    @model_validator(mode="after")
    def validate_not_empty_patch(self):
        if (  # pylint: disable=too-many-boolean-expressions
            self.service_type_id is None
            and self.urban_function_id is None
            and self.radius_availability_meters is None
            and self.time_availability_minutes is None
            and self.services_per_1000_normative is None
            and self.services_capacity_per_1000_normative is None
            and self.is_regulated is None
        ):
            raise ValueError("Empty patch request, invalid")


class NormativeDelete(BaseModel):
    """Normative delete model"""

    service_type_id: int | None = None
    urban_function_id: int | None = None
