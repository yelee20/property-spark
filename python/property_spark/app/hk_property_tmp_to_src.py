from abc import ABC
from argparse import ArgumentError, ArgumentParser, Namespace
from typing import List

from property_spark.app_abc.base_hk_property import Item, SparkBaseAppHKProperty
from property_spark.app_abc.base_tmp_to_src import SparkBaseAppTmpToSrc
from property_spark.constants import DATE_ID_COLUMN_NAME
from property_spark.utils import data_category
# from property_spark.transformation_factory.qoo10_tmp_to_src import (
#     TransformationFactoryQoo10TmpToSrc,
# )
from property_spark.utils.data_category import DataCategory
from property_spark.utils.spark_session import get_spark_session
from pyspark.sql import DataFrame

# TRANSFORMATION = (
#     TransformationFactoryQoo10TmpToSrc().data_category(data_category).build()
# )


class SparkAppHKPropertyTmpToSrc(SparkBaseAppHKProperty, SparkBaseAppTmpToSrc):
    # transformation = TRANSFORMATION
    data_category = DataCategory.ROOM

    def get_arg_parser(self) -> ArgumentParser:
        arg_parser = ArgumentParser()
        arg_parser.add_argument("--operation-date-str", required=True)
        return arg_parser

    def get_src_path(self, args: Namespace) -> str:  # pragma: no cover
        pass

    def get_src_path_tmp_to_src(self, args: Namespace, data_category: DataCategory) -> str:
        path_prefix = self.get_path_prefix()
        return f"{path_prefix}/_tmp/{args.operation_date_str}/{data_category.value}"

    def get_dest_path(self, args: Namespace) -> str:  # pragma: no cover
        pass

    def get_dest_path_tmp_to_src(self, data_category: DataCategory) -> str:
        path_prefix = self.get_path_prefix()
        return f"{path_prefix}/sourcing/{data_category.value}"

    def read(self, path: str) -> DataFrame:
        return self.spark_session.read.format("csv").option("sep", ",").option("header", "true").load(path)

    def write(self, path: str, df: DataFrame) -> None:
        replace_where_clause = self.aggregate_then_get_replace_where_clause(df)
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(DATE_ID_COLUMN_NAME)
            .option("replaceWhere", replace_where_clause)
            .option("mergeSchema", "true")
            .save(path)
        )

    def run(self) -> None:
        arg_parser = self.get_arg_parser()
        args = arg_parser.parse_args()

        src_path = self.get_src_path_tmp_to_src(args, data_category)
        dest_path = self.get_dest_path_tmp_to_src(data_category)

        src_df = self.read(src_path)
        transformed_df = self.transform(src_df)

        self.write(dest_path, transformed_df)


if __name__ == "__main__":  # pragma: no cover
    with get_spark_session("SparkAppHKPropertyTmpToSrc") as spark_session:
        spark_app_hk_property_tmp_to_src = SparkAppHKPropertyTmpToSrc(
            spark_session=spark_session,
        )
        spark_app_hk_property_tmp_to_src.run()
