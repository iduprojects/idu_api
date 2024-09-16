from pydantic import BaseModel, Field

from idu_api.urban_api.dto import TargetProfileDTO


class TargetProfilesData(BaseModel):
    target_profile_id: int = Field(description="target profile identifier", examples=[1])
    name: str = Field(description="target profile name", examples=["ИЖС"])

    @classmethod
    def from_dto(cls, dto: TargetProfileDTO) -> "TargetProfilesData":
        return cls(
            target_profile_id=dto.target_profile_id,
            name=dto.name,
        )


class TargetProfilesPost(BaseModel):
    name: str = Field(..., description="target profile name", examples=["ИЖС"])
