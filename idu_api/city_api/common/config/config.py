import json
import os
from pathlib import Path

from dotenv import load_dotenv


class Config:
    def __init__(self):
        load_dotenv(Path().absolute() / "city_api" / f".env.{os.getenv('APP_ENV')}")
        with open(Path().absolute() / "idu_api" / "city_api" / "city_types.json", "r") as fin:
            self.city_types: list[dict] = json.load(fin)

    @staticmethod
    def get(key: str) -> str:
        return os.getenv(key)


config = Config()
