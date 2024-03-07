import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from dagster import AssetExecutionContext


def connect_to_psql(
    user: str = "root",
    password: str = "root",
    host: str = "localhost",
    port: str = "5433",
    db: str = "store_sales",
) -> Engine:
    """
    Connects to a PostgreSQL database using the provided credentials and returns an Engine object.

    Args:
        user (str): The username for the PostgreSQL database (default is 'root').
        password (str): The password for the PostgreSQL database (default is 'root').
        host (str): The host address of the PostgreSQL database (default is 'localhost').
        port (str): The port number of the PostgreSQL database (default is '5433').
        db (str): The name of the database to connect to (default is 'store_sales').

    Returns:
        Engine: An Engine object connected to the PostgreSQL database.
    """

    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}", echo=True
    )

    return engine


def load_df_to_psql(
    df: pd.DataFrame, table_name: str, engine: Engine, **kwargs
) -> None:
    """
    Load a pandas DataFrame to a PostgreSQL table using the provided engine.

    Args:
        df (pd.DataFrame): The pandas DataFrame to be loaded.
        table_name (str): The name of the table in the PostgreSQL database.
        engine (Engine): The SQLAlchemy engine connected to the PostgreSQL database.
        **kwargs: Additional keyword arguments to be passed to the to_sql method.

    Returns:
        None
    """

    df.to_sql(name=table_name, con=engine, if_exists="append", index=True, **kwargs)


def get_data_partition(
    context: AssetExecutionContext, df: pd.DataFrame
) -> pd.DataFrame:
    """
    Get a data partition from the input DataFrame based on the time window
    defined in the given AssetExecutionContext.

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.
        df: pd.DataFrame - The input DataFrame from which to extract the data partition.

    Returns:
        pd.DataFrame - The data partition extracted from the input DataFrame.
    """

    start, end = context.partition_time_window

    data_partition = df[
        (df.date >= start.strftime("%Y-%m-%d %H:%M:%S"))
        & (df.date < end.strftime("%Y-%m-%d %H:%M:%S"))
    ].copy()

    return data_partition
