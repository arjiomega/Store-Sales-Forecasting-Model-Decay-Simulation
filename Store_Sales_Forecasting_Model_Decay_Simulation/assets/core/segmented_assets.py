import pandas as pd
from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)

from Store_Sales_Forecasting_Model_Decay_Simulation.utils import data_utils


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

    min_date, max_date = data_utils.get_date_range(store_nbr_1_family_grocery_I_df)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")

    metadata = {
        "min_date": MetadataValue.md(f"{min_date}"),
        "max_date": MetadataValue.md(f"{max_date}"),
    }

    context.add_output_metadata(metadata=metadata)

    return store_nbr_1_family_grocery_I_df
