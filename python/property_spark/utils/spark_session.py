from contextlib import contextmanager
from typing import Iterator

# import sentry_sdk  # type: ignore
from property_spark.constants import S3_BUCKET_NAME, DEPLOY_PHASE
from pyspark.sql import SparkSession


@contextmanager
def get_spark_session(
    app_name: str = "AnonymousLaplaceSparkApp",
) -> Iterator[SparkSession]:  # pragma: no cover
    # sentry_sdk.init(
    #     "https://cb1ce2bef1ee4835988be1530a482930@o1096838.ingest.sentry.io/6376120",
    #     environment=DEPLOY_PHASE,
    # )
    spark_session = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # .config(
        #     "spark.sql.warehouse.dir",
        #     f"s3a://{S3_BUCKET_NAME}/warehouse",
        # )
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        yield spark_session
    finally:
        spark_session.stop()
