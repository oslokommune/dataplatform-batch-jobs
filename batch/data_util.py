import re
from dataclasses import dataclass
from urllib.parse import unquote

VALID_STAGES = [
    "incoming",
    "incoming-ng",
    "raw",
    "raw-ng",
    "intermediate",
    "cleaned",
    "processed",
]

VALID_CONFIDENTIALITIES = ["green", "yellow", "red"]


@dataclass
class Column:
    """Utility class representing a typed pandas column."""

    name: str
    dtype: str = "object"
    null: bool = False


@dataclass
class DatasetEntry:
    """An S3 object entry that looks like it belongs to a dataset."""

    stage: str
    confidentiality: str
    dataset_id: str
    version: str
    edition_path: str
    filename: str

    def is_valid(self):
        return (self.stage in VALID_STAGES) and (
            self.confidentiality in VALID_CONFIDENTIALITIES
        )


def extract_key_data(key):
    """Return a DatasetEntry object corresponding to `key`.

    If `key` doesn't look like it belongs to a dataset, return None.
    """
    if isinstance(key, str):
        # Amazon URL-encodes the key twice for unknown reasons, so decode it
        # twice.
        key_unquoted = unquote(unquote(key))

        # fmt: off
        pattern = re.compile("/".join([
            r"(?P<stage>[^/]+)",            # Stage
            r"(?P<confidentiality>[^/]+)",  # Confidentiality
            r"(?P<dataset>\S+)",            # Dataset
            r"version=(?P<version>[^/]+)",  # Version
            r"(?P<edition_path>\S+)",       # Edition path
            r"(?P<filename>.+)$",           # Filename
        ]))
        # fmt: on

        match = pattern.search(key_unquoted)
        if match:
            return DatasetEntry(
                match.group("stage"),
                match.group("confidentiality"),
                match.group("dataset").split("/")[-1],
                match.group("version"),
                match.group("edition_path"),
                match.group("filename"),
            )

    return None
