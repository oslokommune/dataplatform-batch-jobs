from datetime import date

import boto3
import pandas as pd
from moto import mock_s3

from batch.s3_dataset_scanner.scan_s3_objects import scan_s3_objects
from tests.util import mock_byte_output_target

keys = {
    "raw/green/boligpriser-blokkleiligheter/version=1/edition=20200323t190239/boligpriser(2004-2018-v04).xlsx",
    "cleaned/green/botid/version=1/edition=20200207t092904/botid(1.1.2008-1.1.2019-v01).csv",
    "processed/green/bygningstyper-blokk-status/version=1/edition=20190531t082254/00.json",
}


@mock_s3
def test_scan_s3_objects():
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")
    object_body = "foo"

    for key in keys:
        s3.Object("test-input-bucket", key).put(Body=object_body)

    result, output_target = mock_byte_output_target()

    scan_s3_objects(output_target)

    result_data = pd.read_parquet(result)

    assert len(result_data) == len(keys)
    assert set(result_data["key"]) == keys
    assert all([ts.date() == date.today() for ts in result_data["last_modified"]])
    assert sum(result_data["size"]) == len(keys) * len(object_body)
