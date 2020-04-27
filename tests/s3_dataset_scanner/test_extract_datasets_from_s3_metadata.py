from datetime import date
from io import BytesIO
from unittest.mock import Mock

import pandas as pd
from fastparquet import write as pq_write

from batch.models import DatasetOnDate
from batch.s3_dataset_scanner.extract_datasets_from_s3_metadata import (
    extract_datasets_from_s3_metadata,
)


def test_extract_datasets_from_s3_metadata(test_db_session):
    input_df = pd.read_csv("tests/s3_dataset_scanner/data/raw.csv")
    input_data = BytesIO()
    input_data.close = Mock()
    pq_write(input_data, input_df, open_with=lambda io, mode: io)
    input_data.seek(0)
    input_source = Mock(path=input_data)

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
