from typing import Generic, List, TypeVar

from pydantic import BaseModel, Field

FeaturePropertiesType = TypeVar("FeaturePropertiesType")  # pylint: disable=invalid-name


class Page(BaseModel, Generic[FeaturePropertiesType]):
    """
    Pydantic model for pagination
    """

    count: int = Field(description="Total count of records", example=0)
    prev: str | None = Field(
        None,
        description="The path to the previous page",
    )
    next: str | None = Field(
        None,
        description="The path to the next page",
    )
    results: List[FeaturePropertiesType] = Field(
        default_factory=list, description=f"List of {FeaturePropertiesType}", example=[1]
    )
