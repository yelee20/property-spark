import pytest
from property_spark.app.hk_property_tmp_to_src import SparkAppHKPropertyTmpToSrc
from property_spark.utils.data_category import DataCategory
from property_spark.constants import (
    S3_BUCKET_NAME,
    DATA_DIRECTORY,
)
from test.utils import recursive_delete_s3_key

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


@pytest.fixture()
def app(spark_session):
    yield SparkAppHKPropertyTmpToSrc(spark_session=spark_session)


@pytest.fixture()
def dummy_operation_date():
    yield "2022-07-10"


@pytest.fixture()
def dummy_data_id():
    yield 1


@pytest.fixture()
def dummy_data_category():
    yield DataCategory.ROOM


@pytest.fixture()
def args(dummy_operation_date):
    yield [
        "--operation-date-str",
        dummy_operation_date
    ]


@pytest.fixture()
def dummy_src_path(dummy_data_id, dummy_operation_date):
    yield (
        f"s3a://{S3_BUCKET_NAME}"
        f"/{DATA_DIRECTORY}"
        "/_tmp"
        f"/{dummy_operation_date}/room"
    )


@pytest.fixture()
def dummy_src_data_path(
        dummy_src_path,
        spark_session,
        df_hk_property_room_tmp,
        s3,
):
    df_hk_property_room_tmp.write.format("csv").save(dummy_src_path)
    yield dummy_src_path
    recursive_delete_s3_key(s3, "yewon-dev", dummy_src_path)


@pytest.fixture()
def dummy_dest_path(dummy_data_id, dummy_data_category):
    yield (
        f"s3a://{S3_BUCKET_NAME}"
        f"/{DATA_DIRECTORY}"
        "/sourcing/room"
    )


@pytest.fixture()
def sys_args_env(mocker, args):
    mocker.patch("sys.argv", ["hk_property_tmp_to_src.py", *args])
    yield


class TestClassSparkAppHKPropertyTmpToSrc:
    def test_get_arg_parser_success(
            self,
            app,
            args,
            dummy_operation_date,
    ):
        res = app.get_arg_parser()
        ns = res.parse_args(args)
        assert ns.operation_date_str == dummy_operation_date

    def test_get_src_path_success(
            self,
            app,
            args,
            dummy_src_path,
            dummy_data_category,
    ):
        arg_parser = app.get_arg_parser()
        ns = arg_parser.parse_args(args)
        res = app.get_src_path_tmp_to_src(ns, dummy_data_category)
        assert res == dummy_src_path

    def test_get_dest_path_success(
            self,
            app,
            dummy_dest_path,
            dummy_data_category,
    ):
        res = app.get_dest_path_tmp_to_src(dummy_data_category)

        assert res == dummy_dest_path

    def test_run_success(
            self,
            app,
            sys_args_env,
            spark_session,
            dummy_src_data_path,
            dummy_src_path,
            dummy_dest_path,
            df_hk_property_room_src,
    ):
        app.run()
        res_df = spark_session.read.format("delta").load(dummy_dest_path)
        assert len(res_df.schema) == len(df_hk_property_room_src.schema)
        for res_scheme, df_hk_property_room_src_scheme in zip(
                res_df.schema, df_hk_property_room_src.schema
        ):
            assert res_scheme.name == df_hk_property_room_src_scheme.name
            assert res_scheme.dataType == df_hk_property_room_src_scheme.dataType
        assert res_df.collect() == df_hk_property_room_src.collect()

    def test_read_file(self,
                       spark_session,
                       df_hk_property_room_tmp):
        df_hk_property_room_tmp.show()
        app.run()

