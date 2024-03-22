import pandas as pd
from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)

from Store_Sales_Forecasting_Model_Decay_Simulation.assets.forecasting import data_utils
from Store_Sales_Forecasting_Model_Decay_Simulation.assets.forecasting.forecasting import (
    get_date_range,
)


@asset(io_manager_key="local_io_manager")
def store_nbr_1_family_grocery_I(
    context: AssetExecutionContext, combined_data: pd.DataFrame
) -> pd.DataFrame:

    store_nbr_1_family_grocery_I_df, context = data_utils.get_store_and_product_pair(
        context=context, combined_data=combined_data, store_nbr=1, family="GROCERY I"
    )

    store_nbr_1_family_grocery_I_df = data_utils.preprocess_data(
        store_nbr_1_family_grocery_I_df
    )

    min_date, max_date = get_date_range(store_nbr_1_family_grocery_I_df)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")

    metadata = {
        "min_date": MetadataValue.md(f"{min_date}"),
        "max_date": MetadataValue.md(f"{max_date}"),
    }

    context.add_output_metadata(metadata=metadata)

    return store_nbr_1_family_grocery_I_df

    # model_materialization = context.instance.get_latest_materialization_event(
    #     AssetKey("store_nbr_1_family_grocery_I_sales_forecast_model")
    # )

    # reference = store_nbr_1_family_grocery_I_df.copy()

    # metadata_ = {}

    # # No model available
    # if not model_materialization:
    #     current = pd.DataFrame()
    #     metadata_["model materialized"] = MetadataValue.md(f"Not Materialized.")

    # # Model available
    # else:
    #     model_materialization = model_materialization.asset_materialization
    #     min_date = model_materialization.metadata["min_date"].value
    #     max_date = model_materialization.metadata["max_date"].value

    #     # convert type to datetime
    #     min_date = datetime.strptime(min_date, "%Y-%m-%d")
    #     max_date = datetime.strptime(max_date, "%Y-%m-%d")

    #     current = store_nbr_1_family_grocery_I_df[
    #         (store_nbr_1_family_grocery_I_df.date > max_date)
    #     ]

    #     metadata_["model materialized"] = MetadataValue.md(f"Model Materialized.")

    # return (
    #     Output(
    #         reference,
    #         output_name="store_nbr_1_family_grocery_I_reference",
    #         metadata=metadata_,
    #     ),
    #     Output(
    #         current,
    #         output_name="store_nbr_1_family_grocery_I_current",
    #         metadata=metadata_,
    #     ),
    # )
