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

ingest_path = Path(__file__).resolve().parents[3] / 'data/raw_csv/New York_36_transit_census_tract_2022.csv'


# ingest
@asset(
    name="new_york_36_transit_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def new_york_36_transit_census_tract(context: AssetExecutionContext) -> Output[pd.DataFrame]:
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
    ins={"new_york_36_transit_census_tract": AssetIn(key=["new_york_36_transit_census_tract"])},
    name="new_york_36_transit_census_tract_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def new_york_36_transit_census_tract_clean(new_york_36_transit_census_tract) -> Output[pd.DataFrame]:
    df = new_york_36_transit_census_tract
    df = standardize_null_values(df)

    # correct key formats
    df['Census_Tract'] = df['Census_ID'].astype(str)

    # adjust types
    df['Weighted_average_total_jobs'] = df['Weighted_average_total_jobs'].round(0).astype('Int64')

    # pivot
    thresholds = [15, 45, 60]
    # filter for departure time
    df = df[df["Departure"] == "7:00-8:59"]
    # ensure Threshold is numeric
    df = df[["Census_Tract", "Threshold", "Weighted_average_total_jobs"]].copy()
    df["Threshold"] = pd.to_numeric(df["Threshold"], errors="coerce")

    # filter to the requested thresholds
    df = df[df["Threshold"].isin(thresholds)]

    # pivot thresholds into columns
    df = (df
            .pivot(index="Census_Tract",
                   columns="Threshold",
                   values="Weighted_average_total_jobs")
            .reindex(columns=sorted(thresholds))  # ensure only requested thresholds, in order
            .rename_axis(None, axis=1)  # drop column name
            .add_prefix("jobs_at_")  # clearer column names
            .add_suffix("_mins_transit_time")  # clearer column names
            .reset_index())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_new_york_36_transit_census_tract = [new_york_36_transit_census_tract,new_york_36_transit_census_tract_clean]