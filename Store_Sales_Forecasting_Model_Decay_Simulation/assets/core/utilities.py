import numpy as np
import pandas as pd
from dagster import AssetExecutionContext


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
