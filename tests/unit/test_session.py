"""
tests/unit/test_session.py — Unit tests for SparkSession factory.
"""

from streaming.common.session import get_spark


def test_spark_session_creation():
    spark = get_spark("LNTA-TestSession")
    assert spark is not None
    assert spark.sparkContext is not None


def test_spark_session_timezone_utc():
    spark = get_spark("LNTA-TestSession")
    tz = spark.conf.get("spark.sql.session.timeZone")
    assert tz == "UTC"


def test_spark_basic_dataframe_count():
    spark = get_spark("LNTA-TestSession")
    df = spark.range(10)
    assert df.count() == 10
