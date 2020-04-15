import re
from dataclasses import dataclass
from urllib.parse import unquote

import pandas as pd
from fastparquet import write as pq_write


@dataclass
class Column:
    name: str
    dtype: str = "object"
    null: bool = False


# Columns to extract from the incoming CSV file.
columns = [
    Column("time"),
    Column("remote_ip"),
    Column("requester", null=True),
    Column("request_id"),
    Column("operation"),
    Column("key", null=True),
    Column("request_uri", null=True),
    Column("http_status", dtype="float64"),
    Column("error_code", null=True),
    Column("bytes_sent", dtype="float64", null=True),
    Column("object_size", dtype="float64", null=True),
    Column("total_time", dtype="float64", null=True),
    Column("turn_around_time", dtype="float64", null=True),
    Column("user_agent", null=True),
    Column("version_id", null=True),
    Column("host_id", null=True),
    Column("cipher_suite", null=True),
    Column("host_header", null=True),
]

# Additional columns of enriched data to add to the resulting Parquet file.
derived_columns = [
    Column("stage", null=True),
    Column("confidentiality", null=True),
    Column("dataset_id", null=True),
    Column("version", null=True),
    Column("edition_path", null=True),
    Column("filename", null=True),
]


def extract_key_data(key):
    """Return a tuple of interesting fields derived from `key`."""
    stage = None
    confidentiality = None
    dataset_id = None
    version = None
    edition_path = None
    filename = None

    if isinstance(key, str):
        # Amazon URL-encodes the key twice for unknown reasons, so decode it
        # twice.
        key_unquoted = unquote(unquote(key))

        # fmt: off
        pattern = re.compile("/".join([
            r"(?P<stage>[^/]+)",             # Stage
            r"(?P<confidentiality>[^/]+)",   # Confidentiality
            r"(?P<dataset>\S+)",             # Dataset
            r"version=(?P<version>[^/]+)",   # Version
            r"(?P<edition_path>\S+)",        # Edition path
            r"(?P<filename>.+)$",            # Filename
        ]))
        # fmt: on

        match = pattern.search(key_unquoted)
        if match:
            stage = match.group("stage")
            confidentiality = match.group("confidentiality")
            dataset_id = match.group("dataset").split("/")[-1]
            version = match.group("version")
            edition_path = match.group("edition_path")
            filename = match.group("filename")

    return stage, confidentiality, dataset_id, version, edition_path, filename


def row_series_to_columns(series):
    """Turn a pandas series of row tuples into a list of column value tuples."""
    return list(zip(*series))


def enrich_csv(csv_data):
    derived_data = row_series_to_columns(csv_data["key"].map(extract_key_data))

    for i, column in enumerate(derived_columns):
        csv_data[column.name] = derived_data[i] if derived_data else []

    csv_data["time"] = pd.to_datetime(csv_data["time"], format="%d/%b/%Y:%H:%M:%S %z")
    csv_data = csv_data.astype({c.name: c.dtype for c in derived_columns})

    return csv_data


def csv_logs_to_parquet(input_source, output_target):

    with output_target.open("w") as out:
        csv_data = pd.read_csv(
            input_source.open(),
            dtype={c.name: c.dtype for c in columns},
            usecols=[c.name for c in columns],
        )
        enriched_data = enrich_csv(csv_data)

        pq_write(
            out,
            enriched_data,
            has_nulls=[c.name for c in (columns + derived_columns) if c.null],
            times="int96",
            compression="GZIP",
            # We already have an IO-wrapper thanks to Luigi's S3Target. Trick
            # `pq_write` into writing to it instead of a file.
            open_with=lambda io, mode: io,
        )
