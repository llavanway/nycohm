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

ingest_path = Path(__file__).resolve().parents[3] / 'data/raw_csv/DECENNIALPL2020.P1-Data.csv'


# ingest
@asset(
    name="population_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def population_census_tract(context: AssetExecutionContext) -> Output[pd.DataFrame]:
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
    ins={"population_census_tract": AssetIn(key=["population_census_tract"])},
    name="population_census_tract_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def population_census_tract_clean(population_census_tract) -> Output[pd.DataFrame]:
    df = population_census_tract
    df = standardize_null_values(df)

    # drop non-standard rows
    df = df[df['GEO_ID'] != 'Geography']
    df = df[df['GEO_ID'] != '0400000US36']

    # correct key formats
    df['Census_Tract'] = (
        df['GEO_ID'].astype(str)
        .str.extract(r'(\d{11})$', expand=False)
    ).astype(str)

    # adjust types
    df['P1_001N'] = df['P1_001N'].astype('Int64')

    # keep only needed columns
    df = df[['Census_Tract', 'P1_001N']]

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_population_census_tract = [population_census_tract, population_census_tract_clean]