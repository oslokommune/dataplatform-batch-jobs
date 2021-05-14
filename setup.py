from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="dataplatform-batch-jobs",
    version="0.0.1",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Collection of batch jobs for the dataplatform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/dataplatform-batch-jobs",
    packages=["batch"],
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
