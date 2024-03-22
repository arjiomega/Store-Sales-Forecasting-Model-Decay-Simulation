from dagster import load_assets_from_package_module, load_assets_from_modules

from . import forecasting
from .core import partitioned_assets, static_assets, segmented_assets


partitioned_assets_ = load_assets_from_modules(
    modules=[partitioned_assets], group_name="partitioned_assets"
)

static_assets_ = load_assets_from_modules(
    modules=[static_assets], group_name="static_assets"
)

segmented_assets_ = load_assets_from_modules(
    modules=[segmented_assets], group_name="segmented_assets"
)

forecasting_assets = load_assets_from_package_module(
    package_module=forecasting, group_name="forecasting"
)
