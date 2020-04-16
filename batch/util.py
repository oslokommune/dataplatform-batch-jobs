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
