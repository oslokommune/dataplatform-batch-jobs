from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="s3-log-aggregator",
    version="0.0.1",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Batch job for aggregating S3 access logs into datasets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.oslo.kommune.no/origo-dataplatform/s3-log-aggregator",
    packages=["aggregator"],
    install_requires=[
        "alembic",
        "boto3",
        "fastparquet",
        "luigi",
        "psycopg2",
        "s3fs",
        "sqlalchemy",
    ],
)
