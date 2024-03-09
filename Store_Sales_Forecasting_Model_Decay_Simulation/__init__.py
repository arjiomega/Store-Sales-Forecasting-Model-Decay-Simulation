from typing import Optional, Type

import pandas as pd
from dagster import Definitions, EnvVar
from dagster_snowflake import SnowflakeIOManager, SnowflakeResource
from dagster_snowflake_pandas import SnowflakePandasTypeHandler
from dagster_snowflake_pyspark import SnowflakePySparkTypeHandler

from .assets import core_assets
from .schedules import update_frequency

all_assets = [*core_assets]


class SnowflakePandasPySparkIOManager(SnowflakeIOManager):
    @staticmethod
    def type_handlers():
        """type_handlers should return a list of the TypeHandlers that the I/O manager can use.
        Here we return the SnowflakePandasTypeHandler and SnowflakePySparkTypeHandler so that the I/O
        manager can store Pandas DataFrames and PySpark DataFrames.
        """
        return [SnowflakePandasTypeHandler(), SnowflakePySparkTypeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        """If an asset is not annotated with an return type, default_load_type will be used to
        determine which TypeHandler to use to store and load the output.
        In this case, unannotated assets will be stored and loaded as Pandas DataFrames.
        """
        return pd.DataFrame


io_manager = SnowflakePandasPySparkIOManager(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database="STORE_SALES_DB",
    role="ACCOUNTADMIN",
    # warehouse="STORE_SALES_WAREHOUSE",
    schema="STORE_SALES_SCHEMA",
)

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database="STORE_SALES_DB",
    role="ACCOUNTADMIN",
    # warehouse="STORE_SALES_WAREHOUSE",
    schema="STORE_SALES_SCHEMA",
)

defs = Definitions(
    assets=all_assets,
    resources={"io_manager": io_manager, "snowflake_resource": snowflake_resource},
    schedules=[update_frequency],
)
