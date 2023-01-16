import pytest


@pytest.fixture()
def df_hk_property_room_tmp(spark_session, shared_datadir):
    yield spark_session.read.format("csv").option(
        "sep", ",").option("header", "true")\
        .load(
        str(shared_datadir / "hk_property" / "_tmp" / "room")
    )


@pytest.fixture()
def df_hk_property_room_src(spark_session, shared_datadir):
    yield spark_session.read.format("delta").load(
        str(shared_datadir / "hk_property" / "sourcing" / "room")
    )

