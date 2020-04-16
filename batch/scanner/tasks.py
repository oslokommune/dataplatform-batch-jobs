import os
from datetime import datetime

import luigi
from luigi.contrib.s3 import S3Target

from batch.util import getenv
from batch.scanner.scan_s3_objects import scan_s3_objects


def s3_path(prefix, stage, edition):
    return os.path.join(
        getenv("OUTPUT_BUCKET_NAME"),
        prefix,
        stage,
        "red/dataplatform/dataplatform-s3-datasets",
        getenv("INPUT_BUCKET_NAME"),
        "version=1",
        f"edition={edition}/data.parquet.gz",
    )


class ScanS3Objects(luigi.Task):
    """Task for scanning all S3 objects found in the bucket `INPUT_BUCKET_NAME` and
    outputting their metadata in a raw dataset.
    """

    timestamp = luigi.Parameter()
    prefix = luigi.Parameter()

    def run(self):
        scan_s3_objects(self.output())

    def output(self):
        path = s3_path(self.prefix, "raw", self.timestamp)
        return S3Target(f"s3://{path}", format=luigi.format.Nop)


class Run(luigi.Task):
    """Dummy task for kicking off the task chain."""

    prefix = luigi.Parameter(default="")

    def __init__(self, *args, **kwargs):
        now = datetime.utcnow()
        self.timestamp = now.strftime("%Y%m%dT%H%M%S")
        super().__init__(*args, **kwargs)

    def requires(self):
        return ScanS3Objects(timestamp=self.timestamp, prefix=self.prefix)


if __name__ == "__main__":
    luigi.run()
