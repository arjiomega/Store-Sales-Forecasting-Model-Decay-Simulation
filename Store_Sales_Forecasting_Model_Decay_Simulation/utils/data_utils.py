from datetime import datetime

import pandas as pd
from dagster import AssetExecutionContext


def get_date_range(input_df: pd.DataFrame) -> tuple[datetime, datetime]:
    """Get earliest and latest date in input_df.

    Args:
        input_df (pd.DataFrame): Dataframe which we want to get date range

    Returns:
        tuple[datetime,datetime]: earliest and latest dates in our Dataframe
    """

    if "date" in input_df.columns:
        return input_df.date.min(), input_df.date.max()
    else:
        return input_df.index.min(), input_df.index.max()


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


def get_store_and_product_pair(
    context: AssetExecutionContext,
    combined_data: pd.DataFrame,
    store_nbr: int,
    family: str,
    year: int | tuple[int, int] | None = None,
) -> tuple[pd.DataFrame, AssetExecutionContext]:

    match year:
        case int():
            store_and_product_pair_df = combined_data[
                (combined_data["store_nbr"] == store_nbr)
                & (combined_data["family"] == family)
                & (combined_data["date"] <= year)
            ].copy()
        case tuple(int(), int()):
            store_and_product_pair_df = combined_data[
                (combined_data["store_nbr"] == store_nbr)
                & (combined_data["family"] == family)
                & (combined_data["date"] >= year[0])
                & (combined_data["date"] <= year[1])
            ].copy()
        case None:
            store_and_product_pair_df = combined_data[
                (combined_data["store_nbr"] == store_nbr)
                & (combined_data["family"] == family)
            ].copy()
        case _:
            context.log.info(
                "year must be an int or a tuple of ints. Ex. 2015 or (2015, 2020)"
            )

    # Remove unnecessary columns
    store_and_product_pair_df.drop(columns=["store_nbr", "family"], inplace=True)

    return store_and_product_pair_df, context


def generate_time_features(input_df: pd.DataFrame) -> pd.DataFrame:
    """Generate time features."""
    input_df["year"] = input_df.date.dt.year
    input_df["month"] = input_df.date.dt.month
    input_df["day"] = input_df.date.dt.day
    input_df["dayofweek"] = input_df.date.dt.dayofweek
    input_df["dayofyear"] = input_df.date.dt.dayofyear
    input_df["quarter"] = input_df.date.dt.quarter

    return input_df


def boolean_to_int(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df["national_holiday"] = input_df["national_holiday"].astype(int)
    input_df["local_holiday"] = input_df["local_holiday"].astype(int)
    input_df["regional_holiday"] = input_df["regional_holiday"].astype(int)
    return input_df


def preprocess_data(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = generate_time_features(input_df)
    input_df = boolean_to_int(input_df)

    # drop unnecessary columns
    input_df.drop(
        columns=["city", "state", "type", "cluster"],
        inplace=True,
    )

    return input_df
