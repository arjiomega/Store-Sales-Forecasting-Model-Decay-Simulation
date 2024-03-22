from dagster import define_asset_job

from .assets import partitioned_assets_, segmented_assets_, reports_assets

partitioned_load_data_job = define_asset_job(
    "load_data_job", selection=partitioned_assets_
)

segmented_data_job = define_asset_job("segmented_data_job", selection=segmented_assets_)

reports_job = define_asset_job("reports_job", selection=reports_assets)
