from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from functools import partial, reduce
from typing import ClassVar, Generic, TypeVar

from property_spark._typing import Enrichment, Transformation
from property_spark.utils.db_access_info import DBAccessInfo
from pyspark.sql import DataFrame, SparkSession

SrcPath = TypeVar("SrcPath", str, DBAccessInfo)
DestPath = TypeVar("DestPath", str, DBAccessInfo)


@dataclass(frozen=True)
class SparkBaseApp(Generic[SrcPath, DestPath]):
    spark_session: SparkSession
    transformation: ClassVar[Transformation] = ()
    enrichment: ClassVar[Enrichment] = lambda x: ()

    def get_arg_parser(self) -> ArgumentParser:
        raise NotImplementedError()

    def get_src_path(self, args: Namespace) -> SrcPath:
        raise NotImplementedError()

    def get_dest_path(self, args: Namespace) -> DestPath:
        raise NotImplementedError()

    def read(self, path: SrcPath) -> DataFrame:
        raise NotImplementedError()

    def purify_enrichment(self, args: Namespace) -> Transformation:
        return ()

    @staticmethod
    def apply_transformation(
        transformation: Transformation, df: DataFrame
    ) -> DataFrame:
        return reduce(
            lambda acc_df, transform: acc_df.transform(transform), transformation, df
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return type(self).apply_transformation(
            transformation=self.transformation, df=df
        )

    def enrich(self, pure_enrichment: Transformation, df: DataFrame) -> DataFrame:
        return type(self).apply_transformation(transformation=pure_enrichment, df=df)

    def write(self, path: DestPath, df: DataFrame) -> None:
        raise NotImplementedError()

    def run(self) -> None:
        arg_parser = self.get_arg_parser()
        args = arg_parser.parse_args()
        src_path = self.get_src_path(args)
        dest_path = self.get_dest_path(args)
        pure_enrichment = self.purify_enrichment(args)

        read_for_this_session = partial(self.read, src_path)
        write_for_this_session = partial(self.write, dest_path)
        enrich_for_this_session = partial(self.enrich, pure_enrichment)

        src_df = read_for_this_session()
        transformed_df = self.transform(src_df)
        enriched_df = enrich_for_this_session(transformed_df)
        write_for_this_session(enriched_df)
