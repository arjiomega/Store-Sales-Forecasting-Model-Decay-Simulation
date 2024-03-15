from io import StringIO
from datetime import datetime

import pandas as pd
import xgboost as xgb
from sklearn import ensemble
from dagster import (
    asset,
    multi_asset_sensor,
    AssetExecutionContext,
    MetadataValue,
    AssetKey,
    MultiAssetSensorEvaluationContext,
    RunRequest,
)
from dagster_snowflake import SnowflakeResource


from . import data_utils, visualization_utils
from Store_Sales_Forecasting_Model_Decay_Simulation.assets.core import create_tables


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


@asset
def store_nbr_1_family_grocery_I(
    context: AssetExecutionContext, combined_data: pd.DataFrame
) -> pd.DataFrame:

    store_nbr_1_family_grocery_I_df, context = data_utils.get_store_and_product_pair(
        context=context, combined_data=combined_data, store_nbr=1, family="GROCERY I"
    )

    store_nbr_1_family_grocery_I_df = data_utils.preprocess_data(
        store_nbr_1_family_grocery_I_df
    )

    return store_nbr_1_family_grocery_I_df


def _train_forecasting_model(
    training_data: pd.DataFrame, seed: int = 0
) -> ensemble.RandomForestRegressor:
    """Train forecasting model."""

    X, y = training_data.drop(columns=["sales"]), training_data[["sales"]]

    model = xgb.XGBRegressor(n_estimators=100, random_state=seed)
    model.fit(X, y)

    return model


@asset
def store_nbr_1_family_grocery_I_sales_forecast(
    context: AssetExecutionContext, store_nbr_1_family_grocery_I: pd.DataFrame
) -> None:

    store_nbr_1_family_grocery_I.set_index("date", inplace=True)

    model = _train_forecasting_model(training_data=store_nbr_1_family_grocery_I, seed=0)

    context.add_output_metadata(
        metadata={
            "input data preview": MetadataValue.md(
                store_nbr_1_family_grocery_I.head().to_markdown()
            ),
            "feature_importance_plot": visualization_utils.feature_importance_plot(
                model
            ),
            "predict_plot": visualization_utils.predict_plot(
                input_df=store_nbr_1_family_grocery_I, model=model
            ),
            "feature importance": MetadataValue.md(f"{model.feature_importances_}"),
        }
    )
