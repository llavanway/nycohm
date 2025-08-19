import os
from pathlib import Path
import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext
import logging
from .helpers.log_config import configure_logging
from .helpers.prep_for_bq import sanitize_bq_columns

configure_logging()

# ── Settings ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]  # root
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data" / "raw_csv"))
# Map asset names to real CSV file names
CSV_FILE_MAP = {
    "affordable_housing_production_by_building_20250731": "affordable_housing_production_by_building_20250731.csv",
    "enrollment_capacity_and_utilization_reports_20250731": "enrollment_capacity_and_utilization_reports_20250731.csv",
    "housingdb_post2010": "housingdb_post2010.csv",
    "new_york_36_transit_census_tract_2022": "New York_36_transit_census_tract_2022.csv",  # <-- space in filename
}

def _read_csv(context: AssetExecutionContext, asset_name: str) -> tuple[pd.DataFrame, Path]:
    filename = CSV_FILE_MAP[asset_name]
    csv_path = Path(DATA_DIR) / filename
    if not csv_path.exists():
        msg = f"CSV not found: {csv_path}"
        context.log.error(msg)
        raise FileNotFoundError(msg)
    context.log.info(f"Reading CSV: {csv_path}")
    logging.info(f"Reading CSV: {csv_path}")
    return pd.read_csv(csv_path), csv_path

# ── Asset 1: Affordable housing (by building) ──────────────────────────────────
@asset(
    name="affordable_housing_production_by_building_20250731",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="csv_ingest",
)
def affordable_housing_production_by_building_20250731(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "affordable_housing_production_by_building_20250731"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "columns": list(df.columns),
        "project_id_nulls": int(df["Project ID"].isna().sum()) if "Project ID" in df.columns else None,
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": stem,
        "source_owner": "NYC Open Data",
        "notes": "Building-level affordable housing production snapshot as of 2025-07-31.",
    }
    return Output(df, metadata=metadata)

# ── Asset 2: Enrollment capacity & utilization ─────────────────────────────────
@asset(
    name="enrollment_capacity_and_utilization_reports_20250731",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="csv_ingest",
)
def enrollment_capacity_and_utilization_reports_20250731(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "enrollment_capacity_and_utilization_reports_20250731"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": stem,
        "source_owner": "NYC Open Data",
        "notes": "Enrollment, capacity, and utilization metrics as of 2025-07-31.",
    }
    return Output(df, metadata=metadata)

# ── Asset 3: Housing DB (post-2010) ───────────────────────────────────────────
@asset(
    name="housingdb_post2010",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="csv_ingest",
)
def housingdb_post2010(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "housingdb_post2010"
    df, csv_path = _read_csv(context, stem)

    df["Job_Number"] = (
        df["Job_Number"]
        .astype("string")  # pandas nullable string dtype
        .str.strip()  # cleanup for edge cases
    )

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": stem,
        "source_owner": "NYC DCP",
        "notes": "Post-2010 housing permits/records.",
    }
    return Output(df, metadata=metadata)

# ── Asset 4: Transit census tract (NY-36, 2022) ───────────────────────────────
@asset(
    name="new_york_36_transit_census_tract_2022",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="csv_ingest",
)
def new_york_36_transit_census_tract_2022(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "new_york_36_transit_census_tract_2022"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": stem,
        "source_owner": "Univ. of Minnesota",
        "notes": "NY-36 tract-level transit thresholds for 2022.",
    }
    return Output(df, metadata=metadata)


#
# @multi_asset(specs=[AssetSpec("asset_one"), AssetSpec("asset_two")])
# def my_multi_asset():
#     yield MaterializeResult(asset_key="asset_one", metadata={"num_rows": 10})
#     yield MaterializeResult(asset_key="asset_two", metadata={"num_rows": 24})
#
#
# # 1) "Read" asset: pulls from a source table in the configured database.
# @asset(metadata={"csv_path": "/fixed/path/orders.csv"})
# def ingest_df(context) -> pd.DataFrame:
#     csv_path = context.asset_metadata["csv_path"]
#     context.log.info(f"Reading CSV from: {csv_path}")
#     df = pd.read_csv(csv_path)
#     return df
#
# # 2) "Write" asset: the IO manager will persist this DataFrame
# #    to DuckDB OR BigQuery depending on BACKEND.
# @asset(
#     io_manager_key="warehouse_io_manager",
#     metadata={"table": "orders"}  # optional: set the destination table name
# )
# def load_df(source_df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Loader: returns the DataFrame so the IO manager persists it.
#     - In DuckDB: writes to <schema>.orders in the DuckDB file
#     - In BigQuery: writes to <project>.<dataset>.orders
#     """
#     return source_df

#
# @asset(
#     required_resource_keys={"bq_client"},
#     description="Clean and load housing dataset",
# )
# def housing_processed(context):
#     process_housing(context.resources.bq_client)
#
#
# @asset(
#     required_resource_keys={"bq_client"},
#     description="Clean and load affordable housing dataset",
# )
# def affordable_processed(context):
#     process_affordable(context.resources.bq_client)
#
#
# @asset(
#     required_resource_keys={"bq_client"},
#     description="Join processed housing datasets",
# )
# def joined_dataset(context, housing_processed, affordable_processed):
#     join_sets(context.resources.bq_client)
#
#
# @asset(
#     required_resource_keys={"bq_client"},
#     description="Check metrics of the joined dataset",
# )
# def dataset_metrics(context, joined_dataset):
#     check_metrics(context.resources.bq_client)

# keep this list updated
all_assets = [affordable_housing_production_by_building_20250731,
              enrollment_capacity_and_utilization_reports_20250731,
              housingdb_post2010,
              new_york_36_transit_census_tract_2022]

# @resource
# def my_local_duckdb_resource(init_context):
#     # Creates or connects to a local DuckDB file
#     conn = duckdb.connect(database="local.duckdb", read_only=False)
#     return conn
#
# dev_context = build_op_context(resources={"db": my_local_duckdb_resource})
#
# ingest_and_load_csvs(dev_context)
#
# conn = duckdb.connect(database="local.duckdb", read_only=False)
#
# logging.info(conn.execute("""
# SELECT * FROM information_schema.tables""").df().T)
#
# BASE_DIR = Path(__file__).resolve().parents[2]
# logging.info(f'attempting to find data storage directory: {Path(os.getenv("DATA_DIR", BASE_DIR / "data" / "raw_csv"))} ')