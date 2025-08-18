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
from .assets import all_assets
from .resources import DuckDBResource, BigQueryResource

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
        dataset=os.getenv("BQ_DATASET", "analytics"),
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
    assets=all_assets,
    resources={
        "db": db_resource,                          # used for READS
        "warehouse_io_manager": warehouse_io_manager,  # used for WRITES
    },
)