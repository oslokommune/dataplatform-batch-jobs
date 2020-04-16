from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from batch.util import getenv

_ENGINE = None


def get_url():
    """Return a database connection URL."""
    engine = getenv("DB_ENGINE")

    if engine == "postgresql":
        db_user = getenv("DB_USER")
        db_password = getenv("DB_PASSWORD")
        db_host = getenv("DB_HOST")
        db_name = getenv("DB_NAME")
        return f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"

    elif engine == "sqlite-memory":
        return "sqlite:///:memory:"

    raise ValueError(f"Unsupported DB engine: {engine}")


def engine():
    global _ENGINE
    if not _ENGINE:
        _ENGINE = create_engine(get_url())
    return _ENGINE


@contextmanager
def db_session():
    """Provide a transactional scope around a series of DB operations."""
    Session = sessionmaker(bind=engine())
    session = Session()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def update_or_create(session, model, values={}, **query):
    """Convenience method for updating an object with `values`, creating one if
    necessary.

    Inspired by Django's queryset method with the same name.
    """
    obj = session.query(model).filter_by(**query).first()

    if obj:
        for key, value in values.items():
            setattr(obj, key, value)
    else:
        obj = model(**{**query, **values})
        session.add(obj)

    return obj
