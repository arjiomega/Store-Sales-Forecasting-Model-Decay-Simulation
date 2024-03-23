from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from dagster import (
    AssetKey,
    MetadataValue,
    AssetExecutionContext,
    asset,
)
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, TargetDriftPreset, RegressionPreset

from Store_Sales_Forecasting_Model_Decay_Simulation.utils import (
    data_utils,
    visualization_utils,
)


@asset(io_manager_key="local_io_manager")
def reference(
    context: AssetExecutionContext, store_nbr_1_family_grocery_I: pd.DataFrame
) -> pd.DataFrame:

    min_date, max_date = data_utils.get_date_range(store_nbr_1_family_grocery_I)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")

    metadata = {
        "min_date": MetadataValue.md(f"{min_date}"),
        "max_date": MetadataValue.md(f"{max_date}"),
    }

    context.add_output_metadata(metadata=metadata)

    return store_nbr_1_family_grocery_I


@asset(io_manager_key="local_io_manager")
def current(
    context: AssetExecutionContext, store_nbr_1_family_grocery_I: pd.DataFrame
) -> pd.DataFrame:

    reference_materialization = context.instance.get_latest_materialization_event(
        AssetKey("reference")
    )

    # if reference_materialization:
    reference_metadata = reference_materialization.asset_materialization

    min_date_reference = reference_metadata.metadata["min_date"].value
    max_date_reference = reference_metadata.metadata["max_date"].value

    min_date_reference = datetime.strptime(min_date_reference, "%Y-%m-%d")
    max_date_reference = datetime.strptime(max_date_reference, "%Y-%m-%d")

    current_df = store_nbr_1_family_grocery_I[
        store_nbr_1_family_grocery_I.date > max_date_reference
    ].copy()

    min_date, max_date = data_utils.get_date_range(current_df)

    min_date = min_date.strftime("%Y-%m-%d")
    max_date = max_date.strftime("%Y-%m-%d")

    metadata = {
        "min_date": MetadataValue.md(f"{min_date}"),
        "max_date": MetadataValue.md(f"{max_date}"),
    }

    context.add_output_metadata(metadata=metadata)

    return current_df


def _train_forecasting_model(
    training_data: pd.DataFrame, seed: int = 0
) -> xgb.XGBRegressor:
    """Train forecasting model."""

    X, y = training_data.drop(columns=["sales"]), training_data[["sales"]]

    model = xgb.XGBRegressor(n_estimators=100, random_state=seed)
    model.fit(X, y)

    return model


@asset(io_manager_key="local_io_manager")
def train_model(
    context: AssetExecutionContext, reference: pd.DataFrame
) -> xgb.XGBRegressor:

    reference.set_index("date", inplace=True)

    model = _train_forecasting_model(training_data=reference)

    metadata = {
        "feature_importance_plot": visualization_utils.feature_importance_plot(model),
        "predict_plot": visualization_utils.predict_plot(
            input_df=reference, model=model
        ),
    }

    context.add_output_metadata(metadata=metadata)

    return model


def smape(a, f) -> float:
    return 1 / len(a) * np.sum(2 * np.abs(f - a) / (np.abs(a) + np.abs(f)) * 100)


def data_drift_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    column_mapping: ColumnMapping,
) -> tuple[dict, str]:

    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        current_data=current, reference_data=reference, column_mapping=column_mapping
    )
    report = data_drift_report.as_dict()
    drift_detected = (
        "drift detected"
        if report["metrics"][0]["result"]["dataset_drift"]
        else "no drift detected"
    )

    data_drift_report.save_html(filename="reports/data_drift_report.html")

    return report, drift_detected


def regression_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    column_mapping: ColumnMapping,
) -> dict:
    regression_performance = Report(metrics=[RegressionPreset()])
    regression_performance.run(
        current_data=current, reference_data=reference, column_mapping=column_mapping
    )
    regression_report = regression_performance.as_dict()
    regression_performance.save_html(filename="reports/regression_performance.html")

    return regression_report


def target_drift_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
    column_mapping: ColumnMapping,
) -> tuple[dict, str]:
    regression_performance = Report(metrics=[TargetDriftPreset()])
    regression_performance.run(
        current_data=current, reference_data=reference, column_mapping=column_mapping
    )
    target_drift_report = regression_performance.as_dict()

    drift_detected = (
        "drift detected"
        if target_drift_report["metrics"][0]["result"]["drift_detected"]
        else "no drift detected"
    )

    regression_performance.save_html(filename="reports/target_drift_report.html")

    return target_drift_report, drift_detected


@asset(io_manager_key="local_io_manager")
def reports(
    context: AssetExecutionContext,
    reference: pd.DataFrame,
    current: pd.DataFrame,
    train_model: xgb.XGBRegressor,
) -> None:

    reference.set_index("date", inplace=True)
    current.set_index("date", inplace=True)

    current["prediction"] = train_model.predict(current.drop(columns=["sales"]))
    reference["prediction"] = train_model.predict(reference.drop(columns=["sales"]))

    column_mapping = ColumnMapping()

    column_mapping.target = "sales"
    column_mapping.prediction = "prediction"

    data_drift_report_, data_drift_detected = data_drift_report(
        reference, current, column_mapping
    )
    regression_report_ = regression_report(reference, current, column_mapping)
    target_drift_report_, target_drift_detected = target_drift_report(
        reference, current, column_mapping
    )

    reference_smape = smape(reference["sales"], reference["prediction"])
    current_smape = smape(current["sales"], current["prediction"])

    reference_materialization = context.instance.get_latest_materialization_event(
        AssetKey("reference")
    )
    current_materialization = context.instance.get_latest_materialization_event(
        AssetKey("current")
    )

    reference_metadata = reference_materialization.asset_materialization
    current_metadata = current_materialization.asset_materialization

    min_date_reference = reference_metadata.metadata["min_date"].value
    max_date_reference = reference_metadata.metadata["max_date"].value

    min_date_current = current_metadata.metadata["min_date"].value
    max_date_current = current_metadata.metadata["max_date"].value

    metadata = {
        "min_date_reference": MetadataValue.md(f"{min_date_reference}"),
        "max_date_reference": MetadataValue.md(f"{max_date_reference}"),
        "min_date_current": MetadataValue.md(f"{min_date_current}"),
        "max_date_current": MetadataValue.md(f"{max_date_current}"),
        "reference smape": MetadataValue.md(f"{reference_smape}"),
        "current smape": MetadataValue.md(f"{current_smape}"),
        "data drift detected": MetadataValue.md(f"{data_drift_detected}"),
        "target drift detected": MetadataValue.md(f"{target_drift_detected}"),
        "data drift report": MetadataValue.md(f"{data_drift_report_}"),
        "target drift report": MetadataValue.md(f"{target_drift_report_}"),
        "regression report": MetadataValue.md(f"{regression_report_}"),
    }

    context.add_output_metadata(metadata=metadata)
