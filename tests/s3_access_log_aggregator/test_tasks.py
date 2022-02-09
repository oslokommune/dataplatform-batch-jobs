import datetime
from unittest.mock import Mock, patch

import boto3
import luigi
from moto import mock_s3

import batch.s3_access_log_aggregator.tasks
from batch.s3_access_log_aggregator.tasks import (
    AggregateToDB,
    EnrichCSVToParquet,
    S3LogsToCSV,
    past_grace_time,
)


@patch.object(
    batch.s3_access_log_aggregator.tasks, "datetime", Mock(wraps=datetime.datetime)
)
def test_past_grace_time():
    batch.s3_access_log_aggregator.tasks.datetime.utcnow.return_value = (
        datetime.datetime(2020, 3, 1, 8)
    )

    assert not past_grace_time("2020-03-01-8", 5)
    assert not past_grace_time("2020-03-01-7", 65)
    assert not past_grace_time("2020-03-01-6", 125)
    assert past_grace_time("2020-03-01-7", 55)
    assert past_grace_time("2020-03-01-6", 115)
    assert past_grace_time("2020-03-01-5", 175)


@mock_s3
def test_s3_logs_to_csv():
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.create_bucket(Bucket="test-output-bucket")
    with open("tests/s3_access_log_aggregator/data/s3_access_log.txt", "rb") as f:
        s3.Object(
            "test-input-bucket",
            "logs/s3/test-output-bucket/2020-02-13-11-43-07-27B0F6A55F241BF8",
        ).put(Body=f)

    luigi.build([S3LogsToCSV("2020-02-13-11", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv"
    )
    assert len(list(output_objects)) == 1


@mock_s3
@patch.object(
    batch.s3_access_log_aggregator.tasks, "datetime", Mock(wraps=datetime.datetime)
)
def test_s3_logs_to_csv_in_the_future():
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.create_bucket(Bucket="test-output-bucket")
    with open("tests/s3_access_log_aggregator/data/s3_access_log.txt", "rb") as f:
        s3.Object(
            "test-input-bucket",
            "logs/s3/test-output-bucket/2020-02-13-11-43-07-27B0F6A55F241BF8",
        ).put(Body=f)

    # Pretend we're in the past relative to the passed timestamp.
    batch.s3_access_log_aggregator.tasks.datetime.utcnow.return_value = (
        datetime.datetime(2020, 2, 13, 10)
    )
    luigi.build([S3LogsToCSV("2020-02-13-11", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv"
    )
    assert len(list(output_objects)) == 0


@mock_s3
def test_enrich_csv_to_parquet():
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-output-bucket")
    with open("tests/s3_access_log_aggregator/data/raw.csv", "rb") as f:
        s3.Object(
            "test-output-bucket",
            "test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.csv",
        ).put(Body=f)

    EnrichCSVToParquet("2020-02-13-11", "test").run()

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/processed/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour=11/data.parquet"
    )
    assert len(list(output_objects)) == 1


@mock_s3
def test_aggregate_to_db(test_db_session):
    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-output-bucket")
    for hour in range(0, 24):
        with open("tests/s3_access_log_aggregator/data/raw.csv", "rb") as f:
            s3.Object(
                "test-output-bucket",
                f"test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=2/day=13/hour={hour}/data.csv",
            ).put(Body=f)

    luigi.build([AggregateToDB("2020-02-13", "test")], local_scheduler=True)

    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/processed/green/dataplatform/datasett-statistikk-per-dag/version=1/year=2020/month=2/day=13/data-agg.parquet"
    )
    assert len(list(output_objects)) == 1
