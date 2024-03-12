from datetime import datetime

import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    multi_asset_sensor,
    AssetKey,
    MultiAssetSensorEvaluationContext,
    RunRequest,
)


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("store_sales"),
        AssetKey("oil_prices"),
        AssetKey("local_holidays"),
        AssetKey("national_holidays"),
        AssetKey("regional_holidays"),
    ],
    request_assets=[
        AssetKey("combined_data"),
    ],
)
def store_sales_sensor(context: MultiAssetSensorEvaluationContext):
    """Sensor that triggers when the store_sales asset changes."""

    run_requests = []
    TRAIN_DATA_START_DATE = datetime.strptime("2015-01-01", "%Y-%m-%d")

    for (
        partition,
        materializations_by_asset,
    ) in context.latest_materialization_records_by_partition_and_asset().items():

        train_data_start_date_rule = (
            datetime.strptime(partition, "%Y-%m-%d") >= TRAIN_DATA_START_DATE
        )
        materialized_asset_similarity_rule = set(
            materializations_by_asset.keys()
        ) == set(context.asset_keys)

        # check if current partition materializations are all the monitored_assets
        if materialized_asset_similarity_rule and train_data_start_date_rule:
            run_requests.append(RunRequest())
            
            for asset_key, materialization in materializations_by_asset.items():
                context.advance_cursor({asset_key: materialization})

    if not run_requests:
        return run_requests
    else:
        return run_requests[-1]

@asset
def combined_data(
    context: AssetExecutionContext,
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

    merge_1 = pd.merge(store_sales, oil_prices, on="date", how="left")
    merge_2 = pd.merge(merge_1, store_info, on="store_nbr", how="left")
    merge_3 = pd.merge(merge_2, national_holidays, on="date", how="left")
    merge_4 = pd.merge(merge_3, local_holidays, on=["date", "city"], how="left")
    combined_df = pd.merge(merge_4, regional_holidays, on=["date", "state"], how="left")

    context.add_output_metadata(
        metadata={
            "combined_df preview": MetadataValue.md(combined_df.head().to_markdown()),
            "max date": MetadataValue.md(combined_df.date.max().strftime("%Y-%m-%d")),
            "min date": MetadataValue.md(combined_df.date.min().strftime("%Y-%m-%d")),
        }
    )

    return combined_df
