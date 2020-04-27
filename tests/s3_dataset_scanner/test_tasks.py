import datetime
from io import BytesIO
from unittest.mock import Mock, patch

import boto3
import luigi
import pandas as pd
from fastparquet import write as pq_write
from moto import mock_s3

import batch.s3_dataset_scanner.tasks
from batch.models import DatasetOnDate
from batch.s3_dataset_scanner.tasks import ExtractDatasetsFromS3Metadata, ScanS3Objects


@mock_s3
@patch.object(batch.s3_dataset_scanner.tasks, "date", Mock(wraps=datetime.date))
def test_scan_s3_objects():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.create_bucket(Bucket="test-output-bucket")
    s3.Object(
        "test-input-bucket",
        "raw/red/dataplatform/dataplatform-s3-datasets/ok-origo-dataplatform-dev/version=1/edition=20200325T122525/data.parquet.gz",
    ).put()

    batch.s3_dataset_scanner.tasks.date.today.return_value = datetime.date(2020, 1, 1)

    luigi.build(
        [ScanS3Objects(datetime.date(2019, 12, 31), "test")], local_scheduler=True
    )
    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-datasets/test-input-bucket/version=1/year=2019/month=1/day=1/data.parquet.gz"
    )
    assert len(list(output_objects)) == 0

    luigi.build(
        [ScanS3Objects(datetime.date(2020, 1, 1), "test")], local_scheduler=True
    )
    output_objects = s3.Bucket("test-output-bucket").objects.filter(
        Prefix="test/raw/red/dataplatform/dataplatform-s3-datasets/test-input-bucket/version=1/year=2020/month=1/day=1/data.parquet.gz"
    )
    assert len(list(output_objects)) == 1


@mock_s3
def test_extract_datasets_from_s3_metadata(test_db_session):
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-output-bucket")

    input_df = pd.read_csv("tests/s3_dataset_scanner/data/raw.csv")
    input_data = BytesIO()
    input_data.close = Mock()
    pq_write(input_data, input_df, open_with=lambda io, mode: io)
    input_data.seek(0)

    s3.Object(
        "test-output-bucket",
        f"test/raw/red/dataplatform/dataplatform-s3-datasets/test-input-bucket/version=1/year=2020/month=1/day=1/data.parquet.gz",
    ).put(Body=input_data)

    luigi.build(
        [ExtractDatasetsFromS3Metadata(luigi.date_interval.Date(2020, 1, 1), "test")],
        local_scheduler=True,
    )

    result = (
        test_db_session.query(DatasetOnDate.dataset_id)
        .filter(DatasetOnDate.date == datetime.date(2020, 1, 1))
        .all()
    )
    assert set(result) == {
        ("befolkningsframskrivninger",),
        ("redusert-funksjonsevne",),
        ("renovasjonsbiler-status",),
    }
