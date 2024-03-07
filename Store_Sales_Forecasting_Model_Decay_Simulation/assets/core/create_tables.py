"""Create tables for postgresql."""

from sqlalchemy.engine.base import Engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, Float


def create_store_sales_table(engine: Engine, table_name="store_sales"):

    metadata = MetaData()

    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", Date),
        Column("store_nbr", Integer),
        Column("family", String),
        Column("sales", Float),
        Column("onpromotion", Integer),
    )

    metadata.create_all(engine)


def create_oil_prices_table(engine: Engine, table_name="oil_prices"):

    metadata = MetaData()

    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("date", Date),
        Column("dcoilwtico", Float),
    )

    metadata.create_all(engine)


def create_store_info_table(engine: Engine, table_name="store_info"):

    metadata = MetaData()

    Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("store_nbr", Integer),
        Column("city", String),
        Column("state", String),
        Column("type", String),
        Column("cluster", Integer),
    )

    metadata.create_all(engine)
