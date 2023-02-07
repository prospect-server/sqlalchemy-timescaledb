# SQLAlchemy TimescaleDB

[![PyPI version](https://badge.fury.io/py/sqlalchemy-timescaledb.svg)][1]
[![Downloads](https://pepy.tech/badge/sqlalchemy-timescaledb)][2]

This is the TimescaleDB dialect driver for SQLAlchemy.

## Install

```bash
$ pip install sqlalchemy_timescaledb
```

## Usage

Adding to table `timescaledb_hypertable` option allows you to configure the [hypertable parameters][3]:

```Python
import datetime
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, DateTime

engine = create_engine('timescaledb+asyncpg://user:password@0.0.0.0:8001/database')
metadata = MetaData()
metadata.bind = engine

Metric = Table(
    'metric', metadata,
    Column('name', String),
    Column('value', Integer),
    Column('timestamp', DateTime(), default=datetime.datetime.now),
    timescaledb_hypertable={
        'time_column_name': 'timestamp'
    }
)

metadata.create_all(engine)
```

Currently supported drivers: `asyncpg`, `psycopg` and `psycopg2`.


[1]: https://badge.fury.io/py/sqlalchemy-timescaledb
[2]: https://pepy.tech/project/sqlalchemy-timescaledb
[3]: https://docs.timescale.com/api/latest/hypertable/create_hypertable/#optional-arguments