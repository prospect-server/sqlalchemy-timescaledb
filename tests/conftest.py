import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import registry
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from tests.models import Base


def pytest_configure(config):
    path = 'sqlalchemy_timescaledb.dialect'

    registry.register('timescaledb', path, 'TimescaledbPsycopg2Dialect')
    registry.register('timescaledb.psycopg2', path, 'TimescaledbPsycopg2Dialect')
    registry.register('timescaledb.asyncpg', path, 'TimescaledbAsyncpgDialect')


@pytest.fixture(scope='module')
def engine(
        host='0.0.0.0',
        port=8001,
        user='user',
        password='password',
        database='database',
        driver='timescaledb'
):
    return create_engine(
        URL.create(
            host=os.environ.get('POSTGRES_HOST', host),
            port=os.environ.get('POSTGRES_PORT', port),
            username=os.environ.get('POSTGRES_USER', user),
            password=os.environ.get('POSTGRES_PASSWORD', password),
            database=os.environ.get('POSTGRES_DB', database),
            drivername=driver
        )
    )


@pytest.fixture(scope='module')
def session(engine):
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    yield db_session
    db_session.rollback()
    db_session.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def is_hypertable(session):
    def check_hypertable(table):
        return session.execute(
            text(
                f"""
                SELECT count(*)
                FROM _timescaledb_catalog.hypertable
                WHERE table_name = '{table.__tablename__}'
                """
            )
        ).scalar_one() == 1

    return check_hypertable
