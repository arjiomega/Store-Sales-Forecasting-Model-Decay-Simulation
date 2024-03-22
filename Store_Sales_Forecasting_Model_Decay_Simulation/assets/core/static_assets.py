from io import StringIO
from pathlib import Path


import pandas as pd
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    MetadataValue,
    asset,
)
from dagster_snowflake import SnowflakeResource

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
        context: AssetExecutionContext - The context containing the time
                                        window for the data partition.

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


@asset
def combined_data(
    context: AssetExecutionContext,
    snowflake_resource: SnowflakeResource,
    store_sales: pd.DataFrame,
    store_info: pd.DataFrame,
    oil_prices: pd.DataFrame,
    local_holidays: pd.DataFrame,
    national_holidays: pd.DataFrame,
    regional_holidays: pd.DataFrame,
) -> pd.DataFrame:
    """Combine multiple dataframes into a single dataframe for forecasting model
        training.

    Args:
        store_sales (pd.DataFrame): daily sales of a product family at a particular
                                    store including the number of products on promotion.
        store_info (pd.DataFrame): stores' location information.
        oil_prices (pd.DataFrame): oil prices per day in Ecuador.
        local_holidays (pd.DataFrame): local holidays in Ecuador.
        national_holidays (pd.DataFrame): national holidays in Ecuador.
        regional_holidays (pd.DataFrame): regional holidays in Ecuador.
    """

    # Create table if it does not exist
    create_tables.create_table(
        snowflake_resource=snowflake_resource, table_name="combined_data"
    )

    merge_1 = pd.merge(store_sales, oil_prices, on="date", how="left")
    merge_2 = pd.merge(merge_1, store_info, on="store_nbr", how="left")
    merge_3 = pd.merge(merge_2, national_holidays, on="date", how="left")
    merge_4 = pd.merge(merge_3, local_holidays, on=["date", "city"], how="left")
    combined_df = pd.merge(merge_4, regional_holidays, on=["date", "state"], how="left")

    # Fix Nulls
    combined_df.oil_price.fillna(method="bfill", inplace=True)
    combined_df.national_holiday.fillna(False, inplace=True)
    combined_df.local_holiday.fillna(False, inplace=True)
    combined_df.regional_holiday.fillna(False, inplace=True)

    buffer = StringIO()
    combined_df.info(show_counts=True, buf=buffer)
    s = buffer.getvalue()

    context.add_output_metadata(
        metadata={
            "combined_df preview": MetadataValue.md(combined_df.head().to_markdown()),
            "max date": MetadataValue.md(combined_df.date.max().strftime("%Y-%m-%d")),
            "min date": MetadataValue.md(combined_df.date.min().strftime("%Y-%m-%d")),
            "test": MetadataValue.md(s),
        }
    )

    return combined_df
