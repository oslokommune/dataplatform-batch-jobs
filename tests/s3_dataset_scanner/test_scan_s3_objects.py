from io import BytesIO
from unittest.mock import Mock

import boto3
import pandas as pd
from moto import mock_s3

from batch.s3_dataset_scanner.scan_s3_objects import scan_s3_objects


@mock_s3
def test_scan_s3_objects():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")

    for key in [
        "raw/green/boligpriser-blokkleiligheter/version=1/edition=20200323T190239/Boligpriser(2004-2018-v04).xlsx",
        "cleaned/green/botid/version=1/edition=20200207T092904/Botid(1.1.2008-1.1.2019-v01).csv",
        "processed/green/bygningstyper-blokk-status/version=1/edition=20190531T082254/00.json",
    ]:
        s3.Object("test-input-bucket", key).put()

    result = BytesIO()
    # Don't permit actually closing the IO stream, since that will discard the
    # buffer before we get a chance to read it.
    result.close = Mock()
    output_target = Mock(open=Mock(return_value=result))

    scan_s3_objects(output_target)

    result_data = pd.read_parquet(result)

    assert len(result_data) == 3
