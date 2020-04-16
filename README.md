dataplatform-batch-jobs
=======================

Collection of batch jobs for the dataplatform.

## Setup

1. Set up virtualenv:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2. Install Python toolchain: `python3 -m pip install (--user) tox black pip-tools`
   - If running with `--user` flag, add `$HOME/.local/bin` to `$PATH`

## Formatting code

Code is formatted using [black](https://pypi.org/project/black/): `make format`.

## Running tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`.

For tests and linting we use [pytest](https://pypi.org/project/pytest/), [flake8](https://pypi.org/project/flake8/) and [black](https://pypi.org/project/black/).

## Batch jobs

### S3 log aggregator

Batch job that aggregates [S3 access logs](https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html) into a dataset.

#### Running locally

Running the code locally depends on a few environment variables:

```bash
export INPUT_BUCKET_NAME=ok-origo-dataplatform-logs-dev
export OUTPUT_BUCKET_NAME=ok-origo-dataplatform-dev
export DB_ENGINE=postgresql
export DB_USER=<local-postgresql-user>
export DB_PASSWORD=<local-postgresql-password>
export DB_HOST=localhost
export DB_NAME=<local-database-name>
```

Start the Luigi task runner, adjusting the `days` and `prefix` parameters as needed:

```bash
python -m luigi --module batch.aggregator.tasks Run --days 4 --prefix test/my-testing-bucket --local-scheduler
```

### S3 dataset scanner

Batch job for producing a dataset of the dataset IDs present in the dataplatform on a given date.

#### Running locally

Running the code locally depends on a few environment variables:

```bash
export INPUT_BUCKET_NAME=ok-origo-dataplatform-dev
export OUTPUT_BUCKET_NAME=ok-origo-dataplatform-dev
```

Start the Luigi task runner, adjusting `prefix` as needed:

```bash
python -m luigi --module batch.scanner.tasks Run --prefix test/my-testing-bucket --local-scheduler
```

## Deploy

TODO.
