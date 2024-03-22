from datetime import datetime

from dagster import (
    AssetKey,
    EventLogEntry,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    MultiAssetSensorEvaluationContext,
    asset_sensor,
    multi_asset_sensor,
)

from . import jobs


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("store_sales"),
        AssetKey("oil_prices"),
        AssetKey("local_holidays"),
        AssetKey("national_holidays"),
        AssetKey("regional_holidays"),
    ],
    request_assets=[
        AssetKey("combined_data"),
    ],
)
def store_sales_sensor(context: MultiAssetSensorEvaluationContext):
    """Sensor that triggers when the store_sales asset changes."""

    run_requests = []
    TRAIN_DATA_START_DATE = datetime.strptime("2015-01-01", "%Y-%m-%d")

    for (
        partition,
        materializations_by_asset,
    ) in context.latest_materialization_records_by_partition_and_asset().items():

        train_data_start_date_rule = (
            datetime.strptime(partition, "%Y-%m-%d") >= TRAIN_DATA_START_DATE
        )
        materialized_asset_similarity_rule = set(
            materializations_by_asset.keys()
        ) == set(context.asset_keys)

        # check if current partition materializations are all the monitored_assets
        if materialized_asset_similarity_rule and train_data_start_date_rule:
            run_requests.append(RunRequest())

            for asset_key, materialization in materializations_by_asset.items():
                context.advance_cursor({asset_key: materialization})

    if not run_requests:
        return run_requests
    else:
        return run_requests[-1]


@asset_sensor(
    asset_key=AssetKey("combined_data"),
    job=jobs.segmented_data_job,
)
def combined_data_sensor(
    context: SensorEvaluationContext,
    asset_event: EventLogEntry,
):
    """Sensor that triggers when the combined_data asset changes."""

    if asset_event.dagster_event and asset_event.dagster_event.asset_key:
        return RunRequest()
    else:
        return SkipReason(f"Data not yet materialized.")


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("train_model"),
        AssetKey("store_nbr_1_family_grocery_I"),
    ],
    request_assets=[
        AssetKey("reference"),
    ],
)
def train_initial_model_step_1_sensor(context: MultiAssetSensorEvaluationContext):
    """Single use sensor that triggers loading reference/training data."""

    asset_events = context.latest_materialization_records_by_key()

    asset_status = {
        key.to_user_string(): (True if val else False)
        for key, val in asset_events.items()
    }

    data_status = asset_status["store_nbr_1_family_grocery_I"]
    model_status = asset_status["train_model"]

    if data_status and not model_status:
        context.advance_all_cursors()
        return RunRequest()

    elif data_status and model_status:
        return SkipReason(f"Initial model already available.")
    elif not data_status and model_status:
        return SkipReason(
            f"Initial model already available and data not yet materialized."
        )
    else:
        return SkipReason(f"Data not yet materialized.")


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("reference"),
    ],
    request_assets=[
        AssetKey("train_model"),
    ],
)
def train_model_sensor(context: SensorEvaluationContext):
    """Sensor that triggers sales forecasting training
    for initial model.
    """

    asset_events = context.latest_materialization_records_by_key()

    if all(asset_events.values()):
        context.advance_all_cursors()
        return RunRequest()
    else:
        return SkipReason(f"Reference/Training Data Asset not yet materialized.")


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("train_model"),
        AssetKey("current"),
    ],
    request_assets=[
        AssetKey("reports"),
    ],
)
def report_sensor(context: MultiAssetSensorEvaluationContext):
    """Sensor that triggers reports asset"""

    asset_events = context.latest_materialization_records_by_key()

    asset_status = {
        key.to_user_string(): (True if val else False)
        for key, val in asset_events.items()
    }

    if asset_status["current"]:
        context.advance_all_cursors()
        return RunRequest()
    else:
        return SkipReason(f"Current Asset not yet materialized.")


@multi_asset_sensor(
    monitored_assets=[
        AssetKey("reference"),
        AssetKey("train_model"),
        AssetKey("store_nbr_1_family_grocery_I"),
    ],
    request_assets=[
        AssetKey("current"),
    ],
)
def current_sensor(context: MultiAssetSensorEvaluationContext):
    asset_events = context.latest_materialization_records_by_key()

    store_nbr_1_family_grocery_I_materialization = (
        context.instance.get_latest_materialization_event(
            AssetKey("store_nbr_1_family_grocery_I")
        )
    )

    reference_materialization = context.instance.get_latest_materialization_event(
        AssetKey("reference")
    )

    new_data_available = False

    asset_status = {
        key.to_user_string(): (True if val else False)
        for key, val in asset_events.items()
    }

    # check for new data availability
    if store_nbr_1_family_grocery_I_materialization and reference_materialization:
        store_nbr_1_family_grocery_I_metadata = (
            store_nbr_1_family_grocery_I_materialization.asset_materialization
        )

        reference_metadata = reference_materialization.asset_materialization

        min_date_available = store_nbr_1_family_grocery_I_metadata.metadata[
            "min_date"
        ].value
        max_date_available = store_nbr_1_family_grocery_I_metadata.metadata[
            "max_date"
        ].value

        min_date_reference = reference_metadata.metadata["min_date"].value
        max_date_reference = reference_metadata.metadata["max_date"].value

        # convert type to datetime
        min_date_available = datetime.strptime(min_date_available, "%Y-%m-%d")
        max_date_available = datetime.strptime(max_date_available, "%Y-%m-%d")

        min_date_reference = datetime.strptime(min_date_reference, "%Y-%m-%d")
        max_date_reference = datetime.strptime(max_date_reference, "%Y-%m-%d")

        # check if data contains newer data as compared to reference
        if max_date_available > max_date_reference:
            new_data_available = True
        else:
            return SkipReason(
                f"No new data available. "
                f"max_date_available: {max_date_available}, "
                f"max_date_reference: {max_date_reference} "
                f"date difference: {max_date_available - max_date_reference}"
            )

    if asset_status["store_nbr_1_family_grocery_I"] and new_data_available:
        context.advance_all_cursors()
        return RunRequest()
