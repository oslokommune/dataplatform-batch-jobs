from io import BytesIO
from unittest.mock import Mock

import pandas as pd

from aggregator.process_raw import (
    csv_logs_to_parquet,
    enrich_csv,
    extract_key_data,
    row_series_to_columns,
)


def test_extract_key_data():
    (
        stage,
        confidentiality,
        dataset_id,
        version,
        edition_path,
        filename,
    ) = extract_key_data(
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
    input_source = Mock(open=Mock(return_value=open("tests/data/raw.csv")))
    result = BytesIO()
    # Don't permit actually closing the IO stream, since that will discard the
    # buffer before we get a chance to read it.
    result.close = Mock()
    output_target = Mock(open=Mock(return_value=result))

    csv_logs_to_parquet(input_source, output_target)

    result_data = pd.read_parquet(result)
    assert len(result_data) == 3

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
