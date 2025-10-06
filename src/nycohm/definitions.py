"""
# Dev (DuckDB)
export BACKEND=duckdb
export DUCKDB_PATH=dev.duckdb
dagster dev

# Prod (BigQuery)
export BACKEND=bigquery
export BQ_PROJECT=your-project
export BQ_DATASET=your-schema
export GOOGLE_APPLICATION_CREDENTIALS='/Users/lukelavanway/PycharmProjects/nycohm/credentials/bigquery-key.json'
dagster dev
"""

import os
from dagster import Definitions
from src.nycohm.assets.geo_districts import assets_geo_districts
from src.nycohm.assets.main import assets_main
from src.nycohm.assets.housingdb_post2010 import assets_housingdb_post2010
from src.nycohm.assets.affordable_housing_production_by_building import assets_affordable_housing_production_by_building
from src.nycohm.assets.crosswalk_census_tract_to_cc import assets_crosswalk_census_tract_to_cc
from src.nycohm.assets.new_york_36_transit_census_tract import assets_new_york_36_transit_census_tract
from src.nycohm.assets.population_census_tract import assets_population_census_tract
from src.nycohm.assets.crosswalk_census_tract_to_cd import assets_crosswalk_census_tract_to_cd
from .resources import DuckDBResource, BigQueryResource

# set this variable to "bigquery" for prod or "duckdb" for local dev
BACKEND = os.getenv("BACKEND", "duckdb").lower()

# ---- Reading side (resource used inside asset body) ----
if BACKEND == "bigquery":
    db_resource = BigQueryResource(
        project=os.getenv("BQ_PROJECT", "my-project"),
        dataset=os.getenv("BQ_DATASET", "analytics"),
    )
else:
    db_resource = DuckDBResource(
        database=os.getenv("DUCKDB_PATH", "dev.duckdb")
    )

# ---- Writing side (IO manager used by Dagster to persist outputs) ----
if BACKEND == "bigquery":
    # pip install dagster-gcp dagster-gcp-pandas google-cloud-bigquery
    from dagster_gcp_pandas import BigQueryPandasIOManager
    warehouse_io_manager = BigQueryPandasIOManager(
        project=os.getenv("BQ_PROJECT", "nycohm"),
        dataset=os.getenv("BQ_DATASET", "dagster"),
        # write_disposition="WRITE_TRUNCATE",  # or WRITE_APPEND
    )
else:
    # pip install dagster-duckdb dagster-duckdb-pandas duckdb
    from dagster_duckdb_pandas import DuckDBPandasIOManager
    warehouse_io_manager = DuckDBPandasIOManager(
        database=os.getenv("DUCKDB_PATH", "dev.duckdb"),
        schema=os.getenv("DUCKDB_SCHEMA", "main"),
        # create_schema_if_missing=True,
    )

defs = Definitions(
    assets=assets_housingdb_post2010 +
           assets_affordable_housing_production_by_building +
           assets_crosswalk_census_tract_to_cc +
           assets_new_york_36_transit_census_tract +
           assets_population_census_tract +
           assets_crosswalk_census_tract_to_cd +
           assets_geo_districts +
           assets_main
           ,
    resources={
        "db": db_resource,                          # used for READS
        "warehouse_io_manager": warehouse_io_manager,  # used for WRITES
    },
)