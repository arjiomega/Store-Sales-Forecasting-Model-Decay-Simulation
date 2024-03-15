import base64
from io import BytesIO

import pandas as pd
import xgboost as xgb
import matplotlib.pyplot as plt
from dagster import (
    MetadataValue,
    MarkdownMetadataValue,
)


def _plot_to_metadata(fig: plt.Figure) -> MarkdownMetadataValue:
    buffer = BytesIO()
    fig.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    md_plot = MetadataValue.md(md_content)

    return md_plot


def feature_importance_plot(model: xgb.XGBRegressor) -> MarkdownMetadataValue:
    """Get feature importance of forecasting model."""

    feature_importance_df = pd.DataFrame(
        data=model.feature_importances_,
        index=model.feature_names_in_,
        columns=["importance"],
    )

    fig, arr = plt.subplots(1, 1)

    feature_importance_df.sort_values(by="importance").plot(
        kind="barh", title="Feature Importance", ax=arr
    )

    fig.tight_layout()

    return _plot_to_metadata(fig)


def predict_plot(
    input_df: pd.DataFrame, model: xgb.XGBRegressor
) -> MarkdownMetadataValue:

    prediction = model.predict(input_df.drop(columns=["sales"]))

    fig, arr = plt.subplots(1, 1, figsize=(25, 5))

    arr.plot(input_df.index, input_df[["sales"]], label="actual")
    arr.plot(input_df.index, prediction, label="prediction")
    arr.legend(loc="best")

    return _plot_to_metadata(fig)
