import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue, AssetExecutionContext
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.prep_for_bq import sanitize_bq_columns

configure_logging()

# ── Settings ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[3]  # root
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
# Map asset names to real CSV file names
CSV_FILE_MAP = {
    "affordable_housing_production_by_building": Path("raw_csv") / "Affordable_Housing_Production_by_Building_20251001.csv",
    "enrollment_capacity_and_utilization_reports": Path("raw_csv") / "enrollment_capacity_and_utilization_reports_20250731.csv",
    "housingdb_post2010": Path("raw_csv") / "housingdb_post2010.csv",
    "new_york_36_transit_census_tract_2022": Path("raw_csv") / "New York_36_transit_census_tract_2022.csv",  # <-- space in filename,
    "population_census_tract": Path("raw_csv") / "DECENNIALPL2020.P1-Data.csv",
    "crosswalk_census_tract_to_cd": Path("crosswalk") / "nyc_2020_census_tract_nta_cdta_relationships.csv",
    "crosswalk_census_tract_to_cc": Path("crosswalk") / "nyc_2020_census_tract_ccd_2023_relationships.csv",
}

def _read_csv(context: AssetExecutionContext, asset_name: str) -> tuple[pd.DataFrame, Path]:
    relative_path = CSV_FILE_MAP[asset_name]
    csv_path = Path(DATA_DIR) / relative_path
    if not csv_path.exists():
        msg = f"CSV not found: {csv_path}"
        context.log.error(msg)
        raise FileNotFoundError(msg)
    context.log.info(f"Reading CSV: {csv_path}")
    logging.info(f"Reading CSV: {csv_path}")
    return pd.read_csv(csv_path), csv_path

# ── Affordable housing
@asset(
    name="affordable_housing_production_by_building",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def affordable_housing_production_by_building(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "affordable_housing_production_by_building"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "columns": list(df.columns),
        "project_id_nulls": int(df["Project ID"].isna().sum()) if "Project ID" in df.columns else None,
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'affordable_housing_production_by_building',
        "source_owner": "NYC Open Data",
        "notes": "Building-level affordable housing production snapshot as of 2025-07-31.",
    }
    return Output(df, metadata=metadata)

# ── Asset 2: Enrollment capacity & utilization ─────────────────────────────────
@asset(
    name="enrollment_capacity_and_utilization_reports",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def enrollment_capacity_and_utilization_reports(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "enrollment_capacity_and_utilization_reports"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'enrollment_capacity_and_utilization_reports',
        "source_owner": "NYC Open Data",
        "notes": "Enrollment, capacity, and utilization metrics as of 2025-07-31.",
    }
    return Output(df, metadata=metadata)

# ── Housing DB (post-2010)
@asset(
    name="housingdb_post2010",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
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

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'housingdb_post2010',
        "source_owner": "NYC DCP",
        "notes": "Post-2010 housing permits/records.",
    }
    return Output(df, metadata=metadata)

# Jobs accessible via transit by census tract (NY-36, 2022)
@asset(
    name="new_york_36_transit_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def new_york_36_transit_census_tract(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "new_york_36_transit_census_tract_2022"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'new_york_36_transit_census_tract',
        "source_owner": "Univ. of Minnesota",
        "notes": "NY-36 tract-level transit thresholds for 2022.",
    }
    return Output(df, metadata=metadata)


# Population by census tract
@asset(
    name="population_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def population_census_tract(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "population_census_tract"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'new_york_36_transit_census_tract',
        "source_owner": "Univ. of Minnesota",
        "notes": "NY-36 tract-level transit thresholds for 2022.",
    }
    return Output(df, metadata=metadata)


# Crosswalk: census tract to community district
@asset(
    name="crosswalk_census_tract_to_cd",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def crosswalk_census_tract_to_cd(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "crosswalk_census_tract_to_cd"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'new_york_36_transit_census_tract',
        "source_owner": "Univ. of Minnesota",
        "notes": "NY-36 tract-level transit thresholds for 2022.",
    }
    return Output(df, metadata=metadata)

# Crosswalk: census tract to council district
@asset(
    name="crosswalk_census_tract_to_cc",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def crosswalk_census_tract_to_cc(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    stem = "crosswalk_census_tract_to_cc"
    df, csv_path = _read_csv(context, stem)

    df, col_map = sanitize_bq_columns(df, lowercase=False)
    context.log.info(f"Renamed columns for BigQuery: {col_map}")

    now_utc = datetime.now(timezone.utc)
    df["_ingested_at"] = now_utc.isoformat()

    metadata = {
        "csv_path": MetadataValue.path(str(csv_path)),
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        "table": 'new_york_36_transit_census_tract',
        "source_owner": "Univ. of Minnesota",
        "notes": "NY-36 tract-level transit thresholds for 2022.",
    }
    return Output(df, metadata=metadata)


# keep this list updated
assets_ingest = [affordable_housing_production_by_building,
              enrollment_capacity_and_utilization_reports,
              housingdb_post2010,
              new_york_36_transit_census_tract, population_census_tract,
              crosswalk_census_tract_to_cd,
              crosswalk_census_tract_to_cc]