import os
from dataclasses import dataclass


@dataclass
class Column:
    """Utility class representing a typed pandas column."""

    name: str
    dtype: str = "object"
    null: bool = False


def getenv(name):
    """Return the environment variable named `name`, or raise OSError if unset."""
    env = os.getenv(name)

    if env is None:
        raise OSError(f"Environment variable {name} is not set")

    return env


def _timestamp_path(timestamp):
    components = list(map(int, timestamp.split("-")))

    if len(components) == 4:
        path = "year={}/month={}/day={}/hour={}"
    else:
        path = "year={}/month={}/day={}"

    return path.format(*components)


def s3_path(prefix, stage, confidentiality, dataset_id, timestamp, filename):
    return os.path.join(
        getenv("OUTPUT_BUCKET_NAME"),
        prefix,
        stage,
        confidentiality,
        "dataplatform",
        dataset_id,
        "version=1",
        _timestamp_path(timestamp),
        filename,
    )
