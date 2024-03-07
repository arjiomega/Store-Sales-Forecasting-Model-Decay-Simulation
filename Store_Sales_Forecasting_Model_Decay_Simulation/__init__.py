from dagster import (
    FilesystemIOManager,
    Definitions,
)

from .assets import core_assets
from .schedules import update_frequency

all_assets = [*core_assets]

io_manager = FilesystemIOManager(base_dir="data/dagster_data")

defs = Definitions(
    assets=all_assets,
    resources={"io_manager": io_manager},
    schedules=[update_frequency],
)
