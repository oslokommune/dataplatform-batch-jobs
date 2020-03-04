from fastparquet import write as pq_write
import pandas as pd


def read_parquet(input_source):
    """Return a DataFrame of relevant columns from a Parquet file."""
    return pd.read_parquet(input_source.path, columns=["operation", "dataset_id"])


def parquet_logs_to_agg(input_sources, output_target):
    dfs = map(read_parquet, input_sources)

    with output_target.open("w") as out:
        pq_write(
            out,
            count_get_requests(pd.concat(dfs)),
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
