import os
import re
import glob
import pandas as pd
from pathlib import Path
from dagster import multi_asset, AssetOut, Output, MetadataValue, AssetExecutionContext
import logging
from helpers.log_config import configure_logging

configure_logging()

# dev assets

# ── Settings ────────────────────────────────────────────────────────────────────
# Directory to scan for CSVs, e.g. "/data" (can also be set via env)
DATA_DIR = os.getenv("DATA_DIR", "../../data/raw_csv")  # change as needed

def _canonical_table_name(stem: str) -> str:
    """
    Convert a filename stem into a safe table name:
    - lowercase
    - replace non-alphanumerics with underscores
    - collapse doubles and trim underscores
    """
    s = re.sub(r"[^a-zA-Z0-9]+", "_", stem.lower())
    s = re.sub(r"_+", "_", s).strip("_")
    # BigQuery/DuckDB-friendly fallback if empty
    return s or "table"

# Discover CSVs once at import time (static outs for multi_asset)
_csv_paths = sorted(glob.glob(str(Path(DATA_DIR) / "*.csv")))
logging.info(f"Found {len(_csv_paths)} CSV files")
_outs = {
    _canonical_table_name(Path(p).stem): AssetOut(
        io_manager_key="warehouse_io_manager",
        # static metadata is optional; we also attach per-materialization metadata when yielding
        metadata={"discovered_from": p}
    )
    for p in _csv_paths
}
logging.info(f"_outs: {_outs}")

@multi_asset(
    name="ingest_and_load_csvs",
    outs=_outs,
    can_subset=True,  # lets Dagster materialize any subset of these tables
    compute_kind="pandas",
    group_name="csv_ingest"
)
def ingest_and_load_csvs(context: AssetExecutionContext):
    """
    Ingest all CSVs in DATA_DIR and yield one Output per file.
    The IO manager will persist each DataFrame to the warehouse.
      - DuckDB: <schema>.<table>
      - BigQuery: <project>.<dataset>.<table>

    Output names == table names (sanitized file stems).
    """
    if not _csv_paths:
        context.log.warning(f"No CSV files found in: {DATA_DIR}")

    for csv_path in _csv_paths:
        stem = Path(csv_path).stem
        out_name = _canonical_table_name(stem)

        # If this asset run selected a subset, skip non-selected outs
        if context.selected_output_names and out_name not in context.selected_output_names:
            continue

        context.log.info(f"Reading CSV: {csv_path} -> output '{out_name}'")
        df = pd.read_csv(csv_path)

        # Attach rich metadata for lineage/observability & to guide the IO manager
        yield Output(
            df,
            output_name=out_name,
            metadata={
                "csv_path": MetadataValue.path(csv_path),
                "rows": len(df),
                "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
                # Many IO managers read the destination table from metadata; if yours does,
                # this makes the mapping explicit. Otherwise, they can fall back to output_name.
                "table": out_name,
            },
        )


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
all_assets = [ingest_and_load_csvs]

# print(os.getcwd())
#
# print('path:',Path(DATA_DIR))
#
# for i in _csv_paths:
#     print(i)
#
# if not _csv_paths:
#     print(f"No CSV files found in: {DATA_DIR}")
#
# def print_directory_tree(start_path='.', indent=''):
#     for item in os.listdir(start_path):
#         item_path = os.path.join(start_path, item)
#         print(f"{indent}|-- {item}")
#         if os.path.isdir(item_path):
#             print_directory_tree(item_path, indent + '    ')
#
# print(print_directory_tree(start_path='../../data/raw_csv'))