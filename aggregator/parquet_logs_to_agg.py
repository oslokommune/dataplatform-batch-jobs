import pandas as pd
from io import BytesIO
from fastparquet import write as pq_write


def parquet_logs_to_agg(input_source, output_target):
    with output_target.open("w") as out:
        parquet_data = pd.read_parquet(BytesIO(input_source.open().read()))
        df = pd.DataFrame.from_dict(countGetRequests(parquet_data))
        pq_write(
            out,
            df,
            times="int96",
            compression="GZIP",
            # We already have an IO-wrapper thanks to Luigi's S3Target. Trick
            # `pq_write` into writing to it instead of a file.
            open_with=lambda io, mode: io,
        )


def countGetRequests(df):
    df = df[df.operation == "REST.GET.OBJECT"][["dataset_id"]]
    df["count"] = 1
    count = (
        df.groupby(["dataset_id"])
        .count()
        .reset_index()
        .sort_values(["count"], ascending=False)
    )
    return count
