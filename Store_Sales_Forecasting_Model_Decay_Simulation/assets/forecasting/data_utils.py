from dagster import AssetExecutionContext
import pandas as pd


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
