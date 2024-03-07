from datetime import datetime

from dagster import DailyPartitionsDefinition

daily_partitions = DailyPartitionsDefinition(
    start_date=datetime(2013, 1, 1), end_date=datetime(2017, 1, 1), end_offset=1
)
