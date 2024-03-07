from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Date,
    Float,
)


def load_to_psql(df, table_name, user, password, host, port, db):

    # Connect to our postgreSQL
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    metadata = MetaData(engine)

    Variable_tableName = "test_table"

    Table(
        Variable_tableName,
        metadata,
        Column("Id", Integer, primary_key=True, nullable=False),
        Column("Date", Date),
        Column("Country", String),
        Column("Brand", String),
        Column("Price", Float),
    )

    metadata.create_all()
