from sqlalchemy import Column, Date, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()


class DatasetRetrievals(Base):
    __tablename__ = "dataset_retrievals"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String)
    date = Column(Date)
    count = Column(Integer)

    __table_args__ = (UniqueConstraint("dataset_id", "date"),)
