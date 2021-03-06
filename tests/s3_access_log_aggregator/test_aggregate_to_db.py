from io import BytesIO
from unittest.mock import Mock

import pandas as pd
from fastparquet import write as pq_write

from batch.models import DatasetRetrievals
from batch.s3_access_log_aggregator.aggregate_to_db import (
    count_get_requests,
    aggregate_to_db,
    read_parquet,
)


def test_read_parquet():
    input_df = pd.read_csv("tests/s3_access_log_aggregator/data/processed-1.csv")
    input_data = BytesIO()
    # Don't permit actually closing the IO stream, since that will discard the
    # buffer before we get a chance to read it.
    input_data.close = Mock()
    pq_write(input_data, input_df, open_with=lambda io, mode: io)
    input_data.seek(0)
    input_source = Mock(path=input_data)

    df = read_parquet(input_source)

    assert len(df) == 4
    assert "operation" in df
    assert "dataset_id" in df


def test_aggregate_to_db(test_db_session):
    input_df_1 = pd.read_csv("tests/s3_access_log_aggregator/data/processed-1.csv")
    input_df_2 = pd.read_csv("tests/s3_access_log_aggregator/data/processed-2.csv")
    input_data_1 = BytesIO()
    input_data_2 = BytesIO()
    input_data_1.close = Mock()
    input_data_2.close = Mock()
    pq_write(input_data_1, input_df_1, open_with=lambda io, mode: io)
    pq_write(input_data_2, input_df_2, open_with=lambda io, mode: io)
    input_data_1.seek(0)
    input_data_2.seek(0)
    input_source_1 = Mock(path=input_data_1)
    input_source_2 = Mock(path=input_data_2)

    result = BytesIO()
    result.close = Mock()
    output_target = Mock(open=Mock(return_value=result))

    aggregate_to_db([input_source_1, input_source_2], output_target, "2020-03-03")
    df = pd.read_parquet(result)

    assert len(df) == 2
    assert df.loc[df["dataset_id"] == "renovasjonsbiler-status"]["count"].squeeze() == 3
    assert (
        df.loc[df["dataset_id"] == "renovasjonsbiler-status-2"]["count"].squeeze() == 2
    )

    assert test_db_session.query(DatasetRetrievals).count() == 2
    assert (
        test_db_session.query(DatasetRetrievals.count)
        .filter_by(dataset_id="renovasjonsbiler-status")
        .scalar()
        == 3
    )
    assert (
        test_db_session.query(DatasetRetrievals.count)
        .filter_by(dataset_id="renovasjonsbiler-status-2")
        .scalar()
        == 2
    )


def test_count_get_requests():
    d = {
        "dataset_id": ["1234", "5678", "094563", "1234", "5678", "1234"],
        "operation": [
            "REST.PUT.PART",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
            "REST.GET.OBJECT",
        ],
    }
    df = pd.DataFrame(data=d)
    result = count_get_requests(df)
    # Rows
    assert len(result) == 3
    # Count
    assert result.loc[result["dataset_id"] == "1234"]["count"].values[0] == 2
    assert result.loc[result["dataset_id"] == "5678"]["count"].values[0] == 2
    assert result.loc[result["dataset_id"] == "094563"]["count"].values[0] == 1
