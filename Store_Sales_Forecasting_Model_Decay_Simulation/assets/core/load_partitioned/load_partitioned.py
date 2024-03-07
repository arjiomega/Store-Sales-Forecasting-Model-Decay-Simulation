from pathlib import Path

import pandas as pd
from dagster import MetadataValue, AssetExecutionContext, asset

from Store_Sales_Forecasting_Model_Decay_Simulation import config, partitions
from Store_Sales_Forecasting_Model_Decay_Simulation.assets.core import utilities


@asset(partitions_def=partitions.partition)
def load_store_sales_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Load daily sales of a product family at a particular store including the number of products on promotion.

    Data format:
    id: int (ex. 1)
    date: datetime (ex. 2013-01-02)
    store_nbr: int (ex. 1) - identifies the store at which the products are sold.
    family: string (ex. BEVERAGES) - identifies the type of product sold.
    sales: float (ex. 0.0) -    gives the total sales for a product family at a particular
                                store at a given date. Fractional values are possible since
                                products can be sold in fractional units (1.5 kg of cheese,
                                for instance, as opposed to 1 bag of chips).
    onpromotion: int (ex. 0)    gives the total number of items in a product family that
                                were being promoted at a store at a given date.

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.

    Returns:
        pd.DataFrame - The data partition extracted from the input DataFrame.
    """

    store_sales_data_path = Path(config.RAW_DATA_DIR, "train.csv")
    store_sales_data_df = pd.read_csv(
        store_sales_data_path, index_col="id", parse_dates=["date"]
    )

    data_partition = utilities.get_data_partition(context, store_sales_data_df)

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                data_partition.head().to_markdown()
            ),
            "nulls": MetadataValue.md(data_partition.isnull().sum().to_markdown()),
        }
    )

    return data_partition


@asset(partitions_def=partitions.partition)
def load_oil_prices_data(
    context: AssetExecutionContext,
    load_store_sales_data: pd.DataFrame,
) -> pd.DataFrame:
    """Load changes in oil prices per day in Ecuador.

    Data format:
    id: int (ex. 1)
    date: datetime (ex. 2013-01-02)
    dcoilwtico: float (ex. 93.14) - oil price.

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.
        load_store_sales_data (pd.DataFrame): partitioned store sales data.

    Returns:
        pd.DataFrame - The data partition extracted from the input DataFrame.
    """

    oil_data_path = Path(config.RAW_DATA_DIR, "oil.csv")
    oil_data_df = pd.read_csv(oil_data_path, parse_dates=["date"])

    data_partition = utilities.get_data_partition(context, oil_data_df)

    # lets see dates from load_store_sales_data that are not in oil_data_df
    oil_data_missing_dates = load_store_sales_data[
        ~load_store_sales_data.date.isin(data_partition.date)
    ]

    number_of_missing_rows_from_main = len(oil_data_missing_dates)
    number_of_missing_dates_from_main = len(
        load_store_sales_data[
            ~load_store_sales_data.date.isin(data_partition.date)
        ].date.unique()
    )

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                data_partition.head().to_markdown()
            ),
            "nulls": MetadataValue.md(data_partition.isnull().sum().to_markdown()),
            "oil data missing dates preview": MetadataValue.md(
                oil_data_missing_dates.head().to_markdown()
            ),
            "number of missing oil prices rows to main": number_of_missing_rows_from_main,
            "number of missing oil prices dates to main": number_of_missing_dates_from_main,
        }
    )

    return data_partition


@asset(partitions_def=partitions.partition)
def load_transactions_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Total transactions (all product families) of a store per day.

    Data format:
    id: int (ex. 1)
    date: datetime (ex. 2013-01-02)
    store_nbr: int (ex. 1)
    transactions: int (ex. 200)

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.

    Returns:
        pd.DataFrame - The data partition extracted from the input DataFrame.

    NOTE: Will be excluded from preprocessing since forecasting will be done
    separately on each family product and store.
    """

    transactions_data_path = Path(config.RAW_DATA_DIR, "transactions.csv")
    transactions_data_df = pd.read_csv(transactions_data_path, parse_dates=["date"])

    data_partition = utilities.get_data_partition(context, transactions_data_df)

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                data_partition.head().to_markdown()
            ),
            "nulls": MetadataValue.md(data_partition.isnull().sum().to_markdown()),
        }
    )

    return data_partition


@asset(partitions_def=partitions.partition)
def load_holidays_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Load all the holidays in Ecuador (local, regional, national).

    Data format:
    id: int (ex. 1)
    date: datetime (ex. 2012-03-02)
    type: str (ex. Holiday) -   Type of the holiday. Holiday or Transfer. 
                                Some holidays are moved to different date.
    locale: str (ex. Local) - Scale of the holiday. [Local, Regional, National]
    locale_name: str (ex. "Manta") - City or State depending on the locale.
    description: str (ex. "Fundacion de Manta") - Description of the holiday.
    transferred: bool (ex. False) - Specifies if the holiday was transferred.

    Args:
        context: AssetExecutionContext - The context containing the time window for the data partition.

    Returns:
        pd.DataFrame - The data partition extracted from the input DataFrame.
    """
    
    holidays_data_path = Path(config.RAW_DATA_DIR, "holidays_events.csv")
    holidays_data_df = pd.read_csv(holidays_data_path, parse_dates=["date"])

    data_partition = utilities.get_data_partition(context, holidays_data_df)

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                data_partition.head().to_markdown()
            ),
            "nulls": MetadataValue.md(data_partition.isnull().sum().to_markdown()),
        }
    )

    return data_partition


@asset(partitions_def=partitions.partition)
def national_holidays_data(
    context: AssetExecutionContext, load_holidays_data: pd.DataFrame
) -> pd.DataFrame:
    """Extract only national holidays from load_holidays_data.

    Args:
        load_holidays_data (pd.DataFrame): Partitioned holidays data.

    Returns:
        national_holiday_df (pd.DataFrame): Partitioned holidays data containing only national holidays.
    """

    national_holiday_df = load_holidays_data[
        load_holidays_data.locale == "National"
    ].copy()
    national_holiday_df.drop(
        columns=["type", "locale", "locale_name", "description", "transferred"],
        inplace=True,
    )
    national_holiday_df["national_holiday"] = 1

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                national_holiday_df.head().to_markdown()
            ),
            "nulls": MetadataValue.md(national_holiday_df.isnull().sum().to_markdown()),
        }
    )

    return national_holiday_df


@asset(partitions_def=partitions.partition)
def local_holidays_data(
    context: AssetExecutionContext, load_holidays_data: pd.DataFrame
) -> pd.DataFrame:
    """Extract only local holidays from load_holidays_data.

    Args:
        load_holidays_data (pd.DataFrame): Partitioned holidays data.

    Returns:
        local_holiday_df (pd.DataFrame): Partitioned holidays data containing only local holidays.
    """

    local_holiday_df = load_holidays_data[load_holidays_data.locale == "Local"].copy()

    # Drop unnecessary columns
    local_holiday_df.drop(
        columns=["type", "locale", "description", "transferred"], inplace=True
    )

    # Rename column
    local_holiday_df.rename(columns={"locale_name": "city"}, inplace=True)

    # Drop the duplicate
    local_holiday_df.drop_duplicates(inplace=True)

    local_holiday_df["local_holiday"] = 1

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                local_holiday_df.head().to_markdown()
            ),
            "nulls": MetadataValue.md(local_holiday_df.isnull().sum().to_markdown()),
        }
    )

    return local_holiday_df


@asset(partitions_def=partitions.partition)
def regional_holidays_data(
    context: AssetExecutionContext, load_holidays_data: pd.DataFrame
) -> pd.DataFrame:
    """Extract only regional holidays from load_holidays_data.

    Args:
        load_holidays_data (pd.DataFrame): Partitioned holidays data.

    Returns:
        regional_holiday_df (pd.DataFrame): Partitioned holidays data containing only regional holidays.
    """

    regional_holiday_df = load_holidays_data[
        load_holidays_data.locale == "Regional"
    ].copy()

    # Drop unnecessary columns
    regional_holiday_df.drop(
        columns=["type", "locale", "description", "transferred"], inplace=True
    )

    # Rename column
    regional_holiday_df.rename(columns={"locale_name": "state"}, inplace=True)

    # Drop the duplicate
    regional_holiday_df.drop_duplicates(inplace=True)

    regional_holiday_df["regional_holiday"] = 1

    context.add_output_metadata(
        metadata={
            "date partition range": MetadataValue.md(
                f"{context.partition_time_window}"
            ),
            "data partition preview": MetadataValue.md(
                regional_holiday_df.head().to_markdown()
            ),
            "nulls": MetadataValue.md(regional_holiday_df.isnull().sum().to_markdown()),
        }
    )

    return regional_holiday_df
