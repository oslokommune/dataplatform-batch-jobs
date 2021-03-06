from datetime import datetime, timedelta

import luigi
from luigi.contrib.s3 import S3Target

from batch.s3_access_log_aggregator.aggregate_to_db import aggregate_to_db
from batch.s3_access_log_aggregator.enrich_csv_to_parquet import csv_logs_to_parquet
from batch.s3_access_log_aggregator.s3_logs_to_csv import s3_logs_to_csv
from batch.util import s3_path


def past_grace_time(timestamp, min_age):
    """Check whether `timestamp` is at least `min_age` minutes into the past."""
    now = datetime.utcnow()
    ts = datetime.strptime(timestamp, "%Y-%m-%d-%H")

    return now - timedelta(minutes=min_age) > ts


class S3LogsToCSV(luigi.Task):
    """Task for converting S3 access logs with with prefix `timestamp` to CSV and
    uploading the result back to S3 at `output_prefix`. The timestamp should be
    on the form YYYY-MM-DD-HH.
    """

    timestamp = luigi.Parameter()
    output_prefix = luigi.Parameter()
    min_log_age = luigi.IntParameter(default=65)

    def run(self):
        # Don't handle files that are younger than `min_log_age` minutes (65 by
        # default), to make sure that S3 is done populating the logs.
        if past_grace_time(self.timestamp, self.min_log_age):
            s3_logs_to_csv(self.timestamp, self.output())

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


class EnrichCSVToParquet(luigi.Task):
    """Task for enriching the raw CSV logs and converting them to Parquet."""

    timestamp = luigi.Parameter()
    prefix = luigi.Parameter()

    def requires(self):
        return S3LogsToCSV(timestamp=self.timestamp, output_prefix=self.prefix)

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


class AggregateToDB(luigi.Task):
    """Task for aggregating the enriched logs."""

    date = luigi.Parameter()
    prefix = luigi.Parameter()

    def requires(self):
        for hour in range(0, 24):
            yield EnrichCSVToParquet(
                timestamp=f"{self.date}-{hour:02d}", prefix=self.prefix
            )

    def run(self):
        aggregate_to_db(self.input(), self.output(), self.date)

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

    Run jobs for file sets `days` number of days back in time, including the
    current day.
    """

    days = luigi.IntParameter()
    prefix = luigi.Parameter(default="")

    def requires(self):
        now = datetime.utcnow()

        for dt in [now - timedelta(days=x) for x in range(self.days)]:
            yield AggregateToDB(date=dt.strftime("%Y-%m-%d"), prefix=self.prefix)


if __name__ == "__main__":
    luigi.run()
