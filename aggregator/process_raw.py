import re
from urllib.parse import unquote

import pandas as pd
from fastparquet import write as pq_write

# Columns we don't care about. These are dropped when going from CSV to
# Parquet.
insignificant_columns = [
    "bucket_owner",
    "bucket",
    "referer",
    "signature_version",
    "authentication_type",
    "tls_version",
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
            r"(?P<filename>\S+)",            # Filename
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
    new_columns = [
        "stage",
        "confidentiality",
        "dataset_id",
        "version",
        "edition_path",
        "filename",
    ]
    derived_columns = row_series_to_columns(csv_data["key"].map(extract_key_data))

    if derived_columns:
        for i, column in enumerate(new_columns):
            csv_data[column] = derived_columns[i]

    return csv_data


def csv_logs_to_parquet(input_source, output_target):
    with output_target.open("w") as out:
        csv_data = pd.read_csv(
            input_source.open(), usecols=lambda c: c not in insignificant_columns
        )
        enriched_data = enrich_csv(csv_data)

        has_nulls = [
            "requester",
            "key",
            "request_uri",
            "error_code",
            "bytes_sent",
            "object_size",
            "total_time",
            "turn_around_time",
            "user_agent",
            "version_id",
            "host_id",
            "cipher_suite",
            "host_header",
            "stage",
            "confidentiality",
            "dataset_id",
            "version",
            "edition_path",
            "filename",
        ]

        pq_write(
            out,
            enriched_data,
            has_nulls=has_nulls,
            compression="GZIP",
            # We already have an IO-wrapper thanks to Luigi's S3Target. Trick
            # `pq_write` into writing to it instead of a file.
            open_with=lambda io, mode: io,
        )
