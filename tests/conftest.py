import pytest

from batch.db import db_session, engine
from batch.models import Base


@pytest.fixture(scope="function")
def test_db_session():
    with db_session() as session:
        Base.metadata.create_all(engine())
        yield session
        Base.metadata.drop_all(bind=engine())
