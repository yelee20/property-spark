from abc import ABC
from datetime import datetime
from functools import partial
from typing import Any, Callable, Optional, Union

from property_spark.app_abc.base import SparkBaseApp
from property_spark.constants import DATE_ID_COLUMN_NAME
from property_spark.utils.java import bool_to_java_string
from pyspark.sql import Column, DataFrame, GroupedData
from pyspark.sql.functions import max, min


class SparkBaseAppTmpToSrc(SparkBaseApp, ABC):
    @staticmethod
    def get_agg_date_id_str(
        grouped_data: GroupedData,
        agg_func: Callable[[Union[Column, str]], Any],
        field_name: str,
    ) -> Optional[str]:
        datetime_obj: Optional[datetime] = (
            grouped_data.agg(agg_func(DATE_ID_COLUMN_NAME)).first().asDict()[field_name]
        )

        if datetime_obj is None:
            return None

        return datetime_obj.strftime("%Y-%m-%d")

    @staticmethod
    def aggregate_then_get_replace_where_clause(sourcing_df: DataFrame) -> str:
        grouped_df = sourcing_df.groupby()
        get_agg_date_id_str_for_grouped_df = partial(
            SparkBaseAppTmpToSrc.get_agg_date_id_str,
            grouped_df,
        )
        min_max_date_str_pair = tuple(
            get_agg_date_id_str_for_grouped_df(agg_func, field_name)
            for agg_func, field_name in (
                (min, f"min({DATE_ID_COLUMN_NAME})"),
                (max, f"max({DATE_ID_COLUMN_NAME})"),
            )
        )

        if any(date_str is None for date_str in min_max_date_str_pair):
            return bool_to_java_string(False)

        min_date_id_str, max_date_id_str = min_max_date_str_pair

        return (
            f"'{min_date_id_str}' <= {DATE_ID_COLUMN_NAME} AND "
            f"{DATE_ID_COLUMN_NAME} <= '{max_date_id_str}'"
        )
