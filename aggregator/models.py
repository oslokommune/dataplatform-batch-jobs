from sqlalchemy import Column, Date, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()


class DatasetRetrievals(Base):
    """Stores the number of retrievals per dataset on a given date."""

    __tablename__ = "dataset_retrievals"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String)
    date = Column(Date)
    count = Column(Integer)

    __table_args__ = (UniqueConstraint("dataset_id", "date"),)


class DatasetCount(Base):
    """Stores the number of unique datasets present on a given date."""

    __tablename__ = "dataset_count"

    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True)
    count = Column(Integer)
