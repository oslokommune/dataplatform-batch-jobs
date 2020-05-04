from datetime import date

from batch.models import DatasetOnDate
from batch.s3_dataset_scanner.extract_datasets_from_s3_metadata import (
    extract_datasets_from_s3_metadata,
)
from tests.util import csv_file_to_parquet_source


def test_extract_datasets_from_s3_metadata(test_db_session):
    input_source = csv_file_to_parquet_source("tests/s3_dataset_scanner/data/raw.csv")

    extract_datasets_from_s3_metadata(input_source, date(2020, 1, 1))

    result = (
        test_db_session.query(DatasetOnDate.dataset_id)
        .filter(DatasetOnDate.date == date(2020, 1, 1))
        .all()
    )
    assert set(result) == {
        ("befolkningsframskrivninger",),
        ("redusert-funksjonsevne",),
        ("renovasjonsbiler-status",),
    }
