from pathlib import Path

import pandas as pd
from dagster_snowflake import SnowflakeResource
from dagster import asset, MetadataValue, AssetExecutionContext, AutoMaterializePolicy

from Store_Sales_Forecasting_Model_Decay_Simulation import config
from Store_Sales_Forecasting_Model_Decay_Simulation.assets.core import create_tables


@asset(auto_materialize_policy=AutoMaterializePolicy.eager())
def store_info(
    context: AssetExecutionContext, snowflake_resource: SnowflakeResource
) -> pd.DataFrame:
    """load stores' location information.

    Data format:
    id: int (ex. 1)
    store_nbr: int (ex. 1)
    city: string (ex. Quito)
    state: string (ex. Pichincha)
    type: string (ex. D)
    cluster: int (ex. 13)

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.

    Returns:
        pd.DataFrame: Dataframe containing store information

    NOTE:
    Go to the asset in dagster UI and activate auto-materializing.
    """

    # Create table if it does not exist
    create_tables.create_table(
        snowflake_resource=snowflake_resource, table_name="store_info"
    )

    store_info_data_path = Path(config.RAW_DATA_DIR, "stores.csv")
    store_info_data_df = pd.read_csv(store_info_data_path)

    context.add_output_metadata(
        metadata={
            "data preview": MetadataValue.md(store_info_data_df.head().to_markdown()),
            "nulls": MetadataValue.md(store_info_data_df.isnull().sum().to_markdown()),
        }
    )

    return store_info_data_df
