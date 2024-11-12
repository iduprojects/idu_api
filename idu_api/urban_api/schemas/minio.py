"""Schemas connected with MinIO are defined here."""

from pydantic import BaseModel, Field


class MinioImagesURL(BaseModel):

    image_url: str = Field(..., description="minio url to full image")
    preview_url: str = Field(..., description="minio url for preview image")


class MinioImageURL(BaseModel):

    project_id: int = Field(..., description="project identifier")
    url: str = Field(..., description="minio url for preview image")
