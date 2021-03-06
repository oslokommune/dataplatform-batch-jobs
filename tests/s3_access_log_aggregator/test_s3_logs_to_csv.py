import boto3
from moto import mock_s3

from batch.s3_access_log_aggregator.s3_logs_to_csv import _clean_field, s3_logs_to_csv
from tests.util import mock_string_output_target


def test_clean_field():
    # `_clean_field` should just return its argument in most cases ...
    assert _clean_field("") == ""
    assert _clean_field("foo") == "foo"
    # ... but remove stuff that S3 uses to denote "blank".
    assert _clean_field("-") == ""


@mock_s3
def test_s3_logs_to_csv():
    result, output_target = mock_string_output_target()

    s3 = boto3.resource("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-input-bucket")
    with open("tests/s3_access_log_aggregator/data/s3_access_log.txt", "rb") as f:
        s3.Object(
            "test-input-bucket",
            "logs/s3/test-output-bucket/2020-02-13-11-43-07-27B0F6A55F241BF8",
        ).put(Body=f)

    s3_logs_to_csv("2020-02-13-11", output_target)

    with open("tests/s3_access_log_aggregator/data/raw.csv", "r") as expected_result:
        assert result.getvalue() == expected_result.read()
