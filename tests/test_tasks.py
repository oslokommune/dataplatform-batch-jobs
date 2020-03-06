import datetime
from unittest.mock import Mock, patch

import boto3
import luigi
from moto import mock_s3

import aggregator.tasks
from aggregator.tasks import (
    Aggregate,
    ProcessRaw,
    S3LogsToRaw,
    past_grace_time,
    s3_path,
    timestamp_path,
)


def test_timestamp_path():
    assert timestamp_path("2020-03-06") == "year=2020/month=3/day=6"
    assert timestamp_path("2020-03-06-12") == "year=2020/month=3/day=6/hour=12"


def test_s3_path():
    assert (
        s3_path("test", "raw", "red", "dataplatform-s3-logs", "2020-01-01", "data.csv")
        == "test-output-bucket/test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=1/day=1/data.csv"
    )
    assert (
        s3_path(
            "test", "raw", "red", "dataplatform-s3-logs", "2020-01-01-12", "data.csv"
        )
        == "test-output-bucket/test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=1/day=1/hour=12/data.csv"
    )


@patch.object(aggregator.tasks, "datetime", Mock(wraps=datetime.datetime))
def test_past_grace_time():
    aggregator.tasks.datetime.utcnow.return_value = datetime.datetime(2020, 3, 1, 8)

    assert not past_grace_time("2020-03-01-8", 5)
    assert not past_grace_time("2020-03-01-7", 65)
    assert not past_grace_time("2020-03-01-6", 125)
    assert past_grace_time("2020-03-01-7", 55)
    assert past_grace_time("2020-03-01-6", 115)
    assert past_grace_time("2020-03-01-5", 175)


@mock_s3
def test_s3_logs_to_raw():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.create_bucket(Bucket="test-output-bucket")
    s3.Object(
        "test-input-bucket",
        "logs/s3/ok-origo-dataplatform-dev/2020-02-13-11-43-07-27B0F6A55F241BF8",
    ).put(Body=open("tests/data/s3_access_log.txt", "rb"))

    luigi.build([S3LogsToRaw("2020-02-13-11", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv"
    )
    assert len(list(output_objects)) == 1


@mock_s3
@patch.object(aggregator.tasks, "datetime", Mock(wraps=datetime.datetime))
def test_s3_logs_to_raw_in_the_future():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.create_bucket(Bucket="test-output-bucket")
    s3.Object(
        "test-input-bucket",
        "logs/s3/ok-origo-dataplatform-dev/2020-02-13-11-43-07-27B0F6A55F241BF8",
    ).put(Body=open("tests/data/s3_access_log.txt", "rb"))

    # Pretend we're in the past relative to the passed timestamp.
    aggregator.tasks.datetime.utcnow.return_value = datetime.datetime(2020, 2, 13, 10)
    luigi.build([S3LogsToRaw("2020-02-13-11", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv"
    )
    assert len(list(output_objects)) == 0


@mock_s3
def test_process_raw():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-output-bucket")
    s3.Object(
        "test-output-bucket",
        "test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv",
    ).put(Body=open("tests/data/raw.csv", "rb"))

    ProcessRaw("2020-02-13-11", "test").run()

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/processed/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.parquet"
    )
    assert len(list(output_objects)) == 1


@mock_s3
def test_aggregate():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-output-bucket")
    for hour in range(0, 24):
        s3.Object(
            "test-output-bucket",
            f"test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour={hour}/data.csv",
        ).put(Body=open("tests/data/raw.csv", "rb"))

    luigi.build([Aggregate("2020-02-13", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/processed/green/dataplatform/datasett-statistikk-per-dag/version=1/year=2020/month=2/day=13/data-agg.parquet"
    )
    assert len(list(output_objects)) == 1
