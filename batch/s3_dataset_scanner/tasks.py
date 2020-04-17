from datetime import date

import luigi
from luigi.contrib.s3 import S3Target

from batch.s3_dataset_scanner.extract_datasets_from_s3_metadata import (
    extract_datasets_from_s3_metadata,
)
from batch.s3_dataset_scanner.scan_s3_objects import scan_s3_objects
from batch.util import getenv, s3_path


class ScanS3Objects(luigi.Task):
    """Task for scanning all S3 objects found in the bucket `INPUT_BUCKET_NAME` and
    outputting their metadata in a raw dataset.

    The task will only run when the passed date is today. Either way, the
    provided output is an S3 path to an object which may or may not exist.
    """

    date = luigi.DateParameter(default=date.today())
    prefix = luigi.Parameter(default="")

    def run(self):
        if self.date == date.today():
            scan_s3_objects(self.output())

    def output(self):
        input_bucket_name = getenv("INPUT_BUCKET_NAME")
        path = s3_path(
            self.prefix,
            "raw",
            "red",
            f"dataplatform-s3-datasets/{input_bucket_name}",
            self.date.isoformat(),
            "data.parquet.gz",
        )
        return S3Target(f"s3://{path}", format=luigi.format.Nop)


class ExtractDatasetsFromS3Metadata(luigi.Task):
    """Task for processing raw datasets in a given date interval.

    The list of distinct dataset IDs present in the raw dataset on a given date
    are stored in a database, and will later be stored in its own dataset as
    well.
    """

    date = luigi.DateIntervalParameter()
    prefix = luigi.Parameter(default="")

    def requires(self):
        for date in self.date.dates():
            yield ScanS3Objects(date=date, prefix=self.prefix)

    def run(self):
        for req in self.requires():
            extract_datasets_from_s3_metadata(luigi.task.getpaths(req), req.date)


if __name__ == "__main__":
    luigi.run()
