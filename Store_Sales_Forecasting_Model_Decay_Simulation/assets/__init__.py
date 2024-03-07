from dagster import load_assets_from_package_module

from . import core

CORE = "core"

core_assets = load_assets_from_package_module(package_module=core, group_name=CORE)
