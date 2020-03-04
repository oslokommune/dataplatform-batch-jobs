import os
from datetime import datetime, timedelta

import luigi
from luigi.contrib.s3 import S3Target

from aggregator.process_raw import csv_logs_to_parquet
from aggregator.s3_logs_to_raw import s3_logs_to_raw

from aggregator.parquet_logs_to_agg import parquet_logs_to_agg


def timestamp_path(timestamp):
    components = list(map(int, timestamp.split("-")))

    if len(components) == 4:
        path = "year={}/month={}/day={}/hour={}"
    else:
        path = "year={}/month={}/day={}"

    return path.format(*components)


def s3_path(prefix, stage, confidentiality, dataset_id, timestamp, filename):
    bucket_name = os.getenv("OUTPUT_BUCKET_NAME")

    if bucket_name is None:
        raise OSError("Environment variable OUTPUT_BUCKET_NAME is not set")

    return os.path.join(
        bucket_name,
        prefix,
        stage,
        confidentiality,
        "dataplatform",
        dataset_id,
        "version=1",
        timestamp_path(timestamp),
        filename,
    )


class S3LogsToRaw(luigi.Task):
    """Task for converting S3 access logs with with prefix `timestamp` to CSV and
    uploading the result back to S3 at `output_prefix`. The timestamp should be
    on the form YYYY-MM-DD-HH.
    """

    timestamp = luigi.Parameter()
    output_prefix = luigi.Parameter()

    def run(self):
        s3_logs_to_raw(self.timestamp, self.output())

    def output(self):
        path = s3_path(
            self.output_prefix,
            "raw",
            "red",
            "dataplatform-s3-logs",
            self.timestamp,
            "data.csv",
        )
        target = S3Target(f"s3://{path}")
        return target


class ProcessRaw(luigi.Task):
    """Task for enriching the raw CSV logs and converting them to Parquet."""

    timestamp = luigi.Parameter()
    prefix = luigi.Parameter()

    def requires(self):
        return S3LogsToRaw(timestamp=self.timestamp, output_prefix=self.prefix)

    def run(self):
        csv_logs_to_parquet(self.input(), self.output())

    def output(self):
        path = s3_path(
            self.prefix,
            "processed",
            "red",
            "dataplatform-s3-logs",
            self.timestamp,
            "data.parquet.gz",
        )
        target = S3Target(f"s3://{path}", format=luigi.format.Nop)
        return target


class Aggregate(luigi.Task):
    """Task for aggregating the enriched logs."""

    date = luigi.Parameter()
    prefix = luigi.Parameter()

    def requires(self):
        for hour in range(0, 24):
            yield ProcessRaw(timestamp=f"{self.date}-{hour:02d}", prefix=self.prefix)

    def run(self):
        parquet_logs_to_agg(self.input(), self.output())

    def output(self):
        path = s3_path(
            self.prefix,
            "processed",
            "green",
            "datasett-statistikk-per-dag",
            self.date,
            "data-agg.parquet.gz",
        )
        target = S3Target(f"s3://{path}", format=luigi.format.Nop)
        return target


class Run(luigi.Task):
    """Dummy task for kicking off the task chain.

    Run jobs for file sets `hours` number of hours back in time, but skip the
    current hour.
    """

    hours = luigi.IntParameter()
    prefix = luigi.Parameter(default="")

    def requires(self):
        now = datetime.utcnow()

        for dt in [now - timedelta(hours=x) for x in range(1, self.hours + 1)]:
            yield ProcessRaw(timestamp=dt.strftime("%Y-%m-%d-%H"), prefix=self.prefix)


if __name__ == "__main__":
    luigi.run()
