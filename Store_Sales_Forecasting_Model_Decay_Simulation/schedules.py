from datetime import datetime, timedelta

from dagster import (
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
)

from .jobs import partitioned_load_data_job
from .partitions import daily_partitions
from .assets.core.load_data import load_store_sales_data


def get_last_materialized_partition(context, asset) -> datetime:
    instance = context.instance
    asset_key = asset.key
    materialized_partitions = instance.get_materialized_partitions(asset_key)

    DATA_START_DATE = datetime.strptime("2013-01-01", "%Y-%m-%d")

    if not materialized_partitions:
        return DATA_START_DATE
    else:
        previous_partition = datetime.strptime(
            list(materialized_partitions)[-1], "%Y-%m-%d"
        )
        return previous_partition + timedelta(days=1)


@schedule(
    job=partitioned_load_data_job,
    cron_schedule="*/1 * * * *",
    execution_timezone="Asia/Manila",
)
def update_frequency(context: ScheduleEvaluationContext):
    schedule_partition = get_last_materialized_partition(context, load_store_sales_data)

    last_date_partition = datetime.strptime(
        daily_partitions.get_last_partition_key(), "%Y-%m-%d"
    )

    context.log.info(f"schedule partition: {schedule_partition}")
    context.log.info(f"last_date_partition: {last_date_partition}")

    if schedule_partition <= last_date_partition:
        return RunRequest(
            run_key=None, partition_key=schedule_partition.strftime("%Y-%m-%d")
        )
