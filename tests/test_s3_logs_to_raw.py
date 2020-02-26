from io import StringIO
from unittest.mock import Mock

import boto3
from moto import mock_s3

from aggregator.s3_logs_to_raw import _clean_field, s3_logs_to_raw


def test_clean_field():
    # `_clean_field` should just return its argument in most cases ...
    assert _clean_field("") == ""
    assert _clean_field("foo") == "foo"
    # ... but remove stuff that S3 uses to denote "blank".
    assert _clean_field("-") == ""


@mock_s3
def test_s3_logs_to_raw():
    result = StringIO()
    # Don't permit actually closing the IO stream, since that will discard the
    # buffer before we get a chance to read it.
    result.close = Mock()
    output_target = Mock(open=Mock(return_value=result))

    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-input-bucket")
    s3.Object(
        "test-input-bucket",
        "logs/s3/ok-origo-dataplatform-dev/2020-02-13-11-43-07-27B0F6A55F241BF8",
    ).put(Body=open("tests/data/s3_access_log.txt", "rb"))

    s3_logs_to_raw("2020-02-13-11", output_target)

    with open("tests/data/raw.csv", "r") as expected_result:
        assert result.getvalue() == expected_result.read()