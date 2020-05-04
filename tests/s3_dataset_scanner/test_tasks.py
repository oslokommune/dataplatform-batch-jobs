import datetime
from unittest.mock import Mock, patch

import luigi

import batch.s3_dataset_scanner.tasks
from batch.s3_dataset_scanner.tasks import ExtractDatasetsFromS3Metadata, ScanS3Objects


@patch.object(batch.s3_dataset_scanner.tasks, "date", Mock(wraps=datetime.date))
@patch("batch.s3_dataset_scanner.tasks.scan_s3_objects")
def test_scan_s3_objects_run_today(mock_scan_s3_objects):
    batch.s3_dataset_scanner.tasks.date.today.return_value = datetime.date(2020, 1, 1)
    task = ScanS3Objects(datetime.date(2020, 1, 1))

    task.run()

    assert mock_scan_s3_objects.call_count == 1


@patch.object(batch.s3_dataset_scanner.tasks, "date", Mock(wraps=datetime.date))
@patch("batch.s3_dataset_scanner.tasks.scan_s3_objects")
def test_scan_s3_objects_run_not_today(mock_scan_s3_objects):
    batch.s3_dataset_scanner.tasks.date.today.return_value = datetime.date(2019, 12, 31)
    task = ScanS3Objects(datetime.date(2020, 1, 1))

    task.run()

    assert mock_scan_s3_objects.call_count == 0


def test_scan_s3_objects_output():
    task = ScanS3Objects(datetime.date(2020, 1, 1), "test")
    output = task.output()

    assert isinstance(output, luigi.contrib.s3.S3Target)
    assert (
        output.path
        == "s3://test-output-bucket/test/raw/red/dataplatform/dataplatform-s3-datasets/test-input-bucket/version=1/year=2020/month=1/day=1/data.parquet.gz"
    )


def test_extract_datasets_from_s3_metadata_requires():
    task = ExtractDatasetsFromS3Metadata(luigi.date_interval.Month(2020, 1))
    requires = task.requires()

    assert len(list(requires)) == 31
    assert all([isinstance(req, ScanS3Objects) for req in requires])


@patch("batch.s3_dataset_scanner.tasks.extract_datasets_from_s3_metadata")
def test_extract_datasets_from_s3_metadata_run(mock_extract_datasets_from_s3_metadata):
    task = ExtractDatasetsFromS3Metadata(luigi.date_interval.Month(2020, 1))

    task.run()

    assert mock_extract_datasets_from_s3_metadata.call_count == 31
