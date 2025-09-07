import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.prep_for_bq import sanitize_bq_columns

configure_logging()

# Process main housing dataset
@asset(
    ins={"housingdb_post2010": AssetIn(key=["housingdb_post2010"])},
    name="housingdb_post2010_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def housingdb_post2010_clean(housingdb_post2010) -> Output[pd.DataFrame]:
    df = housingdb_post2010

    # drop nulls
    df = df.dropna(subset=['CommntyDst'])
    df = df.dropna(subset=['CouncilDst'])

    # adjust types
    df['CommntyDst'] = df['CommntyDst'].astype('Int64').astype(str)
    df['CouncilDst'] = df['CouncilDst'].astype('Int64').astype(str)
    df['CenTract20'] = df['CenTract20'].astype('Int64').astype(str)
    df['CompltYear'] = df['CompltYear'].astype('Int64').astype(str)
    df['PermitYear'] = df['PermitYear'].astype('Int64').astype(str)

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)

# Process affordable housing dataset
@asset(
    ins={"affordable_housing_production_by_building": AssetIn(key=["affordable_housing_production_by_building"])},
    name="affordable_housing_production_by_building_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def affordable_housing_production_by_building_clean(affordable_housing_production_by_building) -> Output[pd.DataFrame]:
    df = affordable_housing_production_by_building

    # correct key formats
    df['BBL'] = df['BBL'].astype('Int64')

    # Aggregate rows missing BBL into rows not missing BBL by matching Project ID
    logging.info('Sum of units prior to aggregation: {}'.format(df['All_Counted_Units'].sum()))
    missing_bbl_rows = df[df['BBL'].isna()]
    non_missing_bbl_rows = df[~df['BBL'].isna()]
    aggregated_units = missing_bbl_rows.groupby('Project_ID')['All_Counted_Units'].sum()

    non_missing_bbl_rows = non_missing_bbl_rows.merge(
        aggregated_units, on='Project_ID', how='left', suffixes=('', '_missing_bbl')
    )
    non_missing_bbl_rows['All_Counted_Units'] += non_missing_bbl_rows['All_Counted_Units_missing_bbl'].fillna(0)
    non_missing_bbl_rows.drop(columns=['All_Counted_Units_missing_bbl'], inplace=True)

    df = non_missing_bbl_rows

    logging.info('Sum of units after aggregation: {}'.format(df['All_Counted_Units'].sum()))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# Create final joined dataset
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"])},
    name="project",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def project(housingdb_post2010_clean,affordable_housing_production_by_building_clean) -> Output[pd.DataFrame]:
    # Join on BBL
    df = housingdb_post2010_clean.merge(
        affordable_housing_production_by_building_clean, on=['BBL'], how='left'
    )

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# keep this list updated
assets_process = [housingdb_post2010_clean,affordable_housing_production_by_building_clean]