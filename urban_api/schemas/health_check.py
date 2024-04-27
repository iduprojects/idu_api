"""
Ping endpoint response is defined here.
"""
from pydantic import BaseModel, Field


class PingResponse(BaseModel):
    """
    Ping response model, contains default message.
    """

    message: str = Field(default="Pong!")
