import pytest

from aggregator.models import Base
from aggregator.db import db_session, engine


@pytest.fixture(scope="function")
def test_db_session():
    with db_session() as session:
        Base.metadata.create_all(engine())
        yield session
        Base.metadata.drop_all(bind=engine())
