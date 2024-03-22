from datetime import datetime

import pandas as pd


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
