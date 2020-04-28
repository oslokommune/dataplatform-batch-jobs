from batch.data_util import extract_key_data


def test_extract_key_data_valid():
    key = "cleaned/green/befolkningsframskrivninger/version=1/edition=20200207T070503/Befolkningsframskrivning(Fram 2019).csv"
    entry = extract_key_data(key)
    assert entry.is_valid()
    assert entry.stage == "cleaned"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "befolkningsframskrivninger"
    assert entry.version == "1"
    assert entry.edition_path == "edition=20200207T070503"
    assert entry.filename == "Befolkningsframskrivning(Fram 2019).csv"


def test_extract_key_data_filename_with_whitespace():
    key = "cleaned/green/befolkningsframskrivninger/version=1/edition=20200207T070503/Befolkningsframskrivning(Fram 2019).csv"
    entry = extract_key_data(key)
    assert entry.stage == "cleaned"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "befolkningsframskrivninger"
    assert entry.version == "1"
    assert entry.edition_path == "edition=20200207T070503"
    assert entry.filename == "Befolkningsframskrivning(Fram 2019).csv"


def test_extract_key_data_invalid():
    key = "foo/green/levekar-trangbodde-historisk/version=1/edition=20190531T090056/14.json"
    entry = extract_key_data(key)
    assert not entry.is_valid()
    assert entry.stage == "foo"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "levekar-trangbodde-historisk"
    assert entry.version == "1"
    assert entry.edition_path == "edition=20190531T090056"
    assert entry.filename == "14.json"


def test_extract_key_data_non_parsing():
    key = "processed/green/levekar-trangbodde-historisk/15.json"
    assert not extract_key_data(key)
