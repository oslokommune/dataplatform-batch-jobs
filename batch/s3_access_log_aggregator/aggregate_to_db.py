import datetime

import pandas as pd
from fastparquet import write as pq_write

from batch.db import db_session, update_or_create
from batch.models import DatasetRetrievals


def read_parquet(input_source):
    """Return a DataFrame of relevant columns from a Parquet file."""
    return pd.read_parquet(
        input_source.path, columns=["time", "operation", "dataset_id"]
    )


def aggregate_to_db(input_sources, output_target, timestamp):
    dfs = map(read_parquet, input_sources)
    counts = count_get_requests(pd.concat(dfs))
    date = datetime.date.fromisoformat(timestamp)

    with db_session() as session:
        for index, row in counts.iterrows():
            update_or_create(
                session,
                DatasetRetrievals,
                dataset_id=row["dataset_id"],
                date=date,
                values={"count": row["count"]},
            )

    with output_target.open("w") as out:
        pq_write(
            out,
            counts,
            compression="GZIP",
            # We already have an IO-wrapper thanks to Luigi's S3Target. Trick
            # `pq_write` into writing to it instead of a file.
            open_with=lambda io, mode: io,
        )


def count_get_requests(df):
    df = df[df.operation == "REST.GET.OBJECT"][["dataset_id"]]
    df["count"] = 1
    count = (
        df.groupby(["dataset_id"])
        .count()
        .reset_index()
        .sort_values(["count"], ascending=False)
    )
    return count
