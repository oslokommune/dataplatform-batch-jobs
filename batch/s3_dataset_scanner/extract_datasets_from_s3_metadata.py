import pandas as pd

from batch.data_util import extract_key_data
from batch.db import db_session, update_or_create
from batch.models import DatasetOnDate


def extract_datasets_from_s3_metadata(input_source, date):
    """Extract dataset information from a raw dataset of S3 objects.

    TODO: This currently only stores the list of datasets found on the given
    date in a database, but it should be possible to expand this to do more
    interesting things with the dataset later on.
    """
    df = pd.read_parquet(input_source.path)
    dataset_ids = set()

    for key in df["key"]:
        dataset_entry = extract_key_data(key)

        if dataset_entry and dataset_entry.is_valid():
            dataset_ids.add(dataset_entry.dataset_id)

    with db_session() as session:
        for dataset_id in dataset_ids:
            update_or_create(session, DatasetOnDate, date=date, dataset_id=dataset_id)
