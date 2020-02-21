import csv
import os
import re

import boto3

# fmt: off
# Regex for matching fields of log records in the S3 server access log format
# which are always present.
required_fields_pattern = '^' + ' '.join([
    r'(\S+)',                      # Bucket Owner
    r'(\S+)',                      # Bucket
    r'\[([\w:/]+\s[+\-]\d{4})\]',  # Time
    r'(\S+)',                      # Remote IP
    r'(\S+)',                      # Requester
    r'(\S+)',                      # Request ID
    r'(\S+)',                      # Operation
    r'(\S+)',                      # Key
    r'"?(-|[^"]*)"?',              # Request-URI
    r'(\d{3})',                    # HTTP status
    r'(\S+)',                      # Error Code
    r'(\S+)',                      # Bytes Sent
    r'(\S+)',                      # Object Size
    r'(\S+)',                      # Total Time
    r'(\S+)',                      # Turn-Around Time
    r'"?(-|[^"]*)"?',              # Referer
    r'"?(-|[^"]*)"?',              # User-Agent
    r'(\S+)',                      # Version Id
])

# Amazon occasionally add new fields to the access log format, but these aren't
# necessarily present in our older log files, so optionally match these. This
# is designed to be future proof, as long as Amazon stick to their append-only
# policy for new fields.
optional_fields_pattern = ' ?'.join([
    r'(\S+)?',                     # Host Id
    r'(\S+)?',                     # Signature Version
    r'(\S+)?',                     # Cipher Suite
    r'(\S+)?',                     # Authentication Type
    r'(\S+)?',                     # Host Header
    r'(\S+)?',                     # TLS version
])
# fmt: on

compiled_pattern = re.compile(f"{required_fields_pattern} ?{optional_fields_pattern}.*")

log_record_fields = [
    "bucket_owner",
    "bucket",
    "time",
    "remote_ip",
    "requester",
    "request_id",
    "operation",
    "key",
    "request_uri",
    "http_status",
    "error_code",
    "bytes_sent",
    "object_size",
    "total_time",
    "turn_around_time",
    "referer",
    "user_agent",
    "version_id",
    "host_id",
    "signature_version",
    "cipher_suite",
    "authentication_type",
    "host_header",
    "tls_version",
]


def _clean_field(field):
    if field == "-":
        return ""
    return field


def s3_logs_to_raw(timestamp, output_target):
    bucket_name = os.getenv("INPUT_BUCKET_NAME")

    if bucket_name is None:
        raise OSError("Environment variable INPUT_BUCKET_NAME is not set")

    s3 = boto3.resource("s3")
    prefix = f"logs/s3/ok-origo-dataplatform-dev/{timestamp}"

    with output_target.open("w") as out:
        writer = csv.writer(out, dialect="unix")
        writer.writerow(log_record_fields)

        for obj in s3.Bucket(bucket_name).objects.filter(Prefix=prefix):
            body = obj.get()["Body"].read().decode()

            for line in body.strip().split("\n"):
                match = compiled_pattern.search(line)

                # We expect our regex to match every log record. Raise an
                # error if not.
                if not match:
                    raise RuntimeError(f"Couldn't match log record: {line}")

                fields = map(_clean_field, match.groups())
                writer.writerow(fields)
