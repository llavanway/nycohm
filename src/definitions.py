from dagster import Definitions, define_asset_job, load_assets_from_modules
from . import assets
from .helpers.connect_bq import connect_bq

all_assets = load_assets_from_modules([assets])
nyc_housing_job = define_asset_job("nyc_housing_job")

defs = Definitions(
    assets=all_assets,
    jobs=[nyc_housing_job],
    resources={"bq_client": connect_bq},
)