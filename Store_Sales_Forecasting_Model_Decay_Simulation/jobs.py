from dagster import define_asset_job

from .assets import core_partitioned_assets

partitioned_load_data_job = define_asset_job(
    "load_data_job", selection=core_partitioned_assets
)
