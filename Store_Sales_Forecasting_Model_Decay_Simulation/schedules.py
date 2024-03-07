from datetime import datetime
from dateutil.relativedelta import relativedelta

from dagster import (
    ScheduleEvaluationContext,
    AssetsDefinition,
    RunRequest,
    schedule,
)

from .assets.core.load_partitioned import load_partitioned
from .jobs import partitioned_load_data_job
from .partitions import partition



def get_last_materialized_partition(
    context: ScheduleEvaluationContext, 
    asset: AssetsDefinition
    ) -> datetime:
    """Get the last materialized partition for a given asset.

    Returns:
        datetime: starting date for data partition
        
    WARNING:
        Changing partitions definition will break this function.
    """
    instance = context.instance
    asset_key = asset.key

    materialized_partitions = sorted(
        instance.get_materialized_partitions(asset_key),
        key=lambda x: datetime.strptime(x, "%Y-%m-%d"),
    )

    context.log.info(
        f"Materialized partitions: {materialized_partitions}"
    )

    DATA_START_DATE = datetime.strptime("2013-01-01", "%Y-%m-%d")

    if not materialized_partitions:
        return DATA_START_DATE
    else:
        previous_partition = datetime.strptime(
            materialized_partitions[-1], "%Y-%m-%d"
        )
        return previous_partition + relativedelta(months=1)


@schedule(
    job=partitioned_load_data_job,
    cron_schedule="*/1 * * * *",
    execution_timezone="Asia/Manila",
)
def update_frequency(context: ScheduleEvaluationContext):
    schedule_partition = get_last_materialized_partition(
        context, load_partitioned.load_store_sales_data
    )

    last_date_partition = datetime.strptime(
        partition.get_last_partition_key(), "%Y-%m-%d"
    )

    context.log.info(f"schedule partition: {schedule_partition}")
    context.log.info(f"last_date_partition: {last_date_partition}")

    if schedule_partition <= last_date_partition:
        return RunRequest(
            run_key=None, partition_key=schedule_partition.strftime("%Y-%m-%d")
        )
