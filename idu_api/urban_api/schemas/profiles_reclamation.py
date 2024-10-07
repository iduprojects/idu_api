from pydantic import BaseModel, Field

from idu_api.urban_api.dto import ProfilesReclamationDataDTO, ProfilesReclamationDataMatrixDTO
from idu_api.urban_api.schemas.territories import TerritoryShortInfo


class ProfilesReclamationData(BaseModel):
    profile_reclamation_id: int = Field(..., description="id of profile reclamation object", examples=[1])
    source_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated", examples=[1])
    target_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated to", examples=[1])
    territory: TerritoryShortInfo | None
    technical_price: float = Field(
        ..., description="technical price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    technical_time: float = Field(
        ..., description="technical time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_price: float = Field(
        ..., description="biological price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_time: float = Field(
        ..., description="biological time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )

    @classmethod
    def from_dto(cls, dto: ProfilesReclamationDataDTO) -> "ProfilesReclamationData":
        return cls(
            profile_reclamation_id=dto.profile_reclamation_id,
            source_profile_id=dto.source_profile_id,
            target_profile_id=dto.target_profile_id,
            territory=TerritoryShortInfo(
                id=dto.territory_id,
                name=dto.territory_name,
            ) if dto.territory_id is not None else None,
            technical_price=dto.technical_price,
            technical_time=dto.technical_time,
            biological_price=dto.biological_price,
            biological_time=dto.biological_time,
        )


class ProfilesReclamationDataPost(BaseModel):
    source_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated", examples=[1])
    target_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated to", examples=[1])
    territory_id: int | None = Field(None, description="id of territory where profile should be reclamated", examples=[1])
    technical_price: float = Field(
        ..., description="technical price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    technical_time: float = Field(
        ..., description="technical time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_price: float = Field(
        ..., description="biological price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_time: float = Field(
        ..., description="biological time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )


class ProfilesReclamationDataPut(BaseModel):
    source_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated", examples=[1])
    target_profile_id: int = Field(..., description="id of profile (functional zone) to be reclamated to", examples=[1])
    territory_id: int | None = Field(..., description="id of territory where profile should be reclamated", examples=[1])
    technical_price: float = Field(
        ..., description="technical price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    technical_time: float = Field(
        ..., description="technical time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_price: float = Field(
        ..., description="biological price to reclamate source profile (functional zone) to target one", examples=[1.0]
    )
    biological_time: float = Field(
        ..., description="biological time to reclamate source profile (functional zone) to target one", examples=[1.0]
    )


class ProfilesReclamationDataMatrix(BaseModel):
    labels: list[int] = Field(..., description="labels of profiles", examples=[[1, 2]])
    technical_price: list[list[float]] = Field(
        ..., description="technical price matrix", examples=[[[0.0, 1.0], [2.0, 0.0]]]
    )
    technical_time: list[list[float]] = Field(
        ..., description="technical price matrix", examples=[[[0.0, 1.0], [2.0, 0.0]]]
    )
    biological_price: list[list[float]] = Field(
        ..., description="technical price matrix", examples=[[[0.0, 1.0], [2.0, 0.0]]]
    )
    biological_time: list[list[float]] = Field(
        ..., description="technical price matrix", examples=[[[0.0, 1.0], [2.0, 0.0]]]
    )

    @classmethod
    def from_dto(cls, dto: ProfilesReclamationDataMatrixDTO) -> "ProfilesReclamationDataMatrix":
        return cls(
            labels=dto.labels,
            technical_price=dto.technical_price,
            technical_time=dto.technical_time,
            biological_price=dto.biological_price,
            biological_time=dto.biological_time,
        )
