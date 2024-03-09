"""Changing the partition definition will break schedules.
    If needed to be changed, also update schedules to fit the
    partition.
"""

from datetime import datetime

from dagster import MonthlyPartitionsDefinition

partition = MonthlyPartitionsDefinition(
    start_date=datetime(2013, 1, 1),
    end_date=datetime(2017, 9, 1),
    timezone="Asia/Manila",
    end_offset=1,
)
