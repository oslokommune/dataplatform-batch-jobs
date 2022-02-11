from unittest.mock import Mock

import pandas as pd

from batch.s3_access_log_aggregator.enrich_csv_to_parquet import (
    csv_logs_to_parquet,
    enrich_csv,
    extract_key_data_to_tuple,
    row_series_to_columns,
)
from tests.util import mock_byte_output_target


def test_extract_key_data_to_tuple():
    (
        stage,
        confidentiality,
        dataset_id,
        version,
        edition_path,
        filename,
    ) = extract_key_data_to_tuple(
        "raw/green/renovasjonsbiler-status/version%253D1/year%253D2020/month%253D2/day%253D17/"
        "dp.green.renovasjonsbiler-status.raw.1.json-1-2020-02-17-06-24-39-85a538c5-365a-4d73-ae44-0c2496058747"
    )
    assert stage == "raw"
    assert confidentiality == "green"
    assert dataset_id == "renovasjonsbiler-status"
    assert version == "1"
    assert edition_path == "year=2020/month=2/day=17"
    assert (
        filename
        == "dp.green.renovasjonsbiler-status.raw.1.json-1-2020-02-17-06-24-39-85a538c5-365a-4d73-ae44-0c2496058747"
    )


def test_extract_key_data_to_tuple_invalid():
    assert extract_key_data_to_tuple("foo/bar") == (None, None, None, None, None, None)


def test_row_series_to_columns():
    assert row_series_to_columns(pd.Series([(1, 4), (2, 5), (3, 6)])) == [
        (1, 2, 3),
        (4, 5, 6),
    ]


def test_enrich_csv():
    key = "raw/yellow/deichman-koha/deichman-koha-avdelinger/version%253D1/latest/part.1.parq"
    data = pd.DataFrame.from_dict(
        {"time": None, "foo_col": [1], "key": [key], "bar_col": [2]}
    )
    enriched_data = enrich_csv(data)
    row = enriched_data.loc[0]
    assert row.stage == "raw"
    assert row.confidentiality == "yellow"
    assert row.dataset_id == "deichman-koha-avdelinger"
    assert row.version == "1"
    assert row.edition_path == "latest"
    assert row.filename == "part.1.parq"


def test_csv_logs_to_parquet():
    result, output_target = mock_byte_output_target()

    with open("tests/s3_access_log_aggregator/data/raw.csv") as f:
        input_source = Mock(open=Mock(return_value=f))
        csv_logs_to_parquet(input_source, output_target)

    result_data = pd.read_parquet(result)
    assert len(result_data) == 4

    # Verify the absence of some irrelevant columns.
    assert "signature_version" not in result_data
    assert "tls_version" not in result_data

    sample_row = result_data.loc[0]
    assert sample_row["time"] == pd.Timestamp("2020-02-17T06:31:44", tz="UTC")
    assert sample_row["stage"] == "raw"
    assert sample_row["confidentiality"] == "green"
    assert sample_row["dataset_id"] == "renovasjonsbiler-status"
    assert sample_row["version"] == "1"
    assert sample_row["edition_path"] == "year=2020/month=2/day=17"
    assert (
        sample_row["filename"]
        == "dp.green.renovasjonsbiler-status.raw.1.json-1-2020-02-17-06-24-39-85a538c5-365a-4d73-ae44-0c2496058747"
    )

    assert result_data.dtypes["time"] == "datetime64[ns, UTC]"
    assert result_data.dtypes["operation"] == "object"
    assert result_data.dtypes["http_status"] == "float64"
    assert result_data.dtypes["error_code"] == "object"
    assert result_data.dtypes["bytes_sent"] == "float64"
    assert result_data.dtypes["object_size"] == "float64"
    assert result_data.dtypes["stage"] == "object"
    assert result_data.dtypes["filename"] == "object"


def test_empty_csv_logs_to_parquet():
    result, output_target = mock_byte_output_target()

    with open("tests/s3_access_log_aggregator/data/raw-empty.csv") as f:
        input_source = Mock(open=Mock(return_value=f))
        csv_logs_to_parquet(input_source, output_target)

    result_data = pd.read_parquet(result)

    assert len(result_data) == 0
    assert result_data.dtypes["operation"] == "object"
    assert result_data.dtypes["http_status"] == "float64"
    assert result_data.dtypes["stage"] == "object"
