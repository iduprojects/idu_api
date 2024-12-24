import os
from pathlib import Path

from dotenv import load_dotenv


class Config:  # pylint: disable=too-few-public-methods
    def __init__(self):
        load_dotenv(Path().absolute() / "city_api" / f".env.{os.getenv('APP_ENV')}")

    @staticmethod
    def get(key: str) -> str:
        return os.getenv(key)


config = Config()
