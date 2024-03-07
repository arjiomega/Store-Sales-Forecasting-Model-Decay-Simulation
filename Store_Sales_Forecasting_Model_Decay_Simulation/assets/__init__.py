from dagster import load_assets_from_package_module

from . import core

from .core import load_partitioned

CORE = "core"

core_assets = load_assets_from_package_module(package_module=core, group_name=CORE)

core_partitioned_assets = load_assets_from_package_module(
    package_module=load_partitioned, group_name="partitioned_core"
)
