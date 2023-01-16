from abc import ABC
from enum import Enum, unique

from property_spark.app_abc.base import SparkBaseApp
from property_spark.constants import (
    S3_BUCKET_NAME,
    DATA_DIRECTORY,
)


@unique
class Item(Enum):
    ROOM = "room"


class SparkBaseAppHKProperty(SparkBaseApp, ABC):
    @staticmethod
    def get_path_prefix(
    ) -> str:
        return (
            f"s3a://{S3_BUCKET_NAME}"
            f"/{DATA_DIRECTORY}"
        )
