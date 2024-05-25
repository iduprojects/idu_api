from typing import Generic, List, TypeVar

from pydantic import BaseModel, Field

FeaturePropertiesType = TypeVar("FeaturePropertiesType")


class Page(BaseModel, Generic[FeaturePropertiesType]):
    """
    Pydantic model for pagination
    """

    count: int = Field(description="Total count of records", example=0)
    prev: str = Field(default="", description="The path to the previous page")
    next: str = Field(default="", description="The path to the next page")
    results: List[FeaturePropertiesType] = Field(
        default=[], description=f"List of {FeaturePropertiesType}", example=[1]
    )
