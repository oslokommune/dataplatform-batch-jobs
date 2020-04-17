import boto3
import pandas as pd
from fastparquet import write as pq_write

from batch.data_util import Column
from batch.util import getenv

columns = [
    Column("key"),
    Column("last_modified", dtype="datetime64[ns, utc]"),
    Column("size", dtype="int64"),
]


def _s3_object_summary_to_list(obj_s):
    """Return a list of interesting metadata from an S3 object summary `obj_s`."""
    return [obj_s.key, obj_s.last_modified, obj_s.size]


def scan_s3_objects(output_target):
    bucket_name = getenv("INPUT_BUCKET_NAME")
    s3 = boto3.resource("s3")
    objects = s3.Bucket(bucket_name).objects.all()

    data = map(_s3_object_summary_to_list, objects)
    column_names = [c.name for c in columns]
    column_types = {c.name: c.dtype for c in columns}

    df = pd.DataFrame(data, columns=column_names).astype(column_types)

    with output_target.open("w") as out:
        pq_write(
            out,
            df,
            has_nulls=[c.name for c in columns if c.null],
            compression="GZIP",
            # We already have an IO-wrapper thanks to Luigi's S3Target. Trick
            # `pq_write` into writing to it instead of a file.
            open_with=lambda io, mode: io,
        )
