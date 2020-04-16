from datetime import date

import luigi
from luigi.contrib.s3 import S3Target

from batch.s3_dataset_scanner.scan_s3_objects import scan_s3_objects
from batch.util import getenv, s3_path


class ScanS3Objects(luigi.Task):
    """Task for scanning all S3 objects found in the bucket `INPUT_BUCKET_NAME` and
    outputting their metadata in a raw dataset.
    """

    prefix = luigi.Parameter()

    def run(self):
        scan_s3_objects(self.output())

    def output(self):
        input_bucket_name = getenv("INPUT_BUCKET_NAME")
        path = s3_path(
            self.prefix,
            "raw",
            "red",
            f"dataplatform-s3-datasets/{input_bucket_name}",
            date.today().isoformat(),
            "data.parquet.gz",
        )
        return S3Target(f"s3://{path}", format=luigi.format.Nop)


class Run(luigi.Task):
    """Dummy task for kicking off the task chain."""

    prefix = luigi.Parameter(default="")

    def requires(self):
        return ScanS3Objects(prefix=self.prefix)


if __name__ == "__main__":
    luigi.run()
