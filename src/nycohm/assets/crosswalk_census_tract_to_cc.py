from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.prep_for_bq import sanitize_bq_columns
from src.nycohm.helpers._read_csv import _read_csv
from src.nycohm.helpers.handle_null import standardize_null_values

configure_logging()

ingest_path = Path(__file__).resolve().parents[3] / 'data/crosswalk/nyc_2020_census_tract_ccd_2023_relationships.csv'

# ingest
@asset(
    name="crosswalk_census_tract_to_cc",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def crosswalk_census_tract_to_cc(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    df, csv_path = _read_csv(context, ingest_path)

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


# process
@asset(
    ins={"crosswalk_census_tract_to_cc": AssetIn(key=["crosswalk_census_tract_to_cc"])},
    name="crosswalk_census_tract_to_cc_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def crosswalk_census_tract_to_cc_clean(crosswalk_census_tract_to_cc) -> Output[pd.DataFrame]:
    df = crosswalk_census_tract_to_cc
    df = standardize_null_values(df)

    logging.info(df.head(20).to_string())

    # correct key formats
    df['Council_District'] = df['CCD2023'].astype('Int64')
    df['Census_Tract']= df['GEOID'].astype(str)

    # keep only needed columns
    df = df[['GEOID', 'Council_District', 'Census_Tract']]

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_crosswalk_census_tract_to_cc = [crosswalk_census_tract_to_cc,crosswalk_census_tract_to_cc_clean]