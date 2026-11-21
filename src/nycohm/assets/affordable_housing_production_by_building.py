import os
import pandas as pd
from datetime import datetime, timezone
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.prep_for_bq import sanitize_bq_columns
from src.nycohm.helpers._read_csv import _read_csv
from src.nycohm.helpers.handle_null import standardize_null_values
from src.nycohm.helpers.make_full_census_tract import make_full_census_tract
from src.nycohm.fetch_data import ensure_dataset

configure_logging()

DATASET = 'affordable_housing_production_by_building'

# ingest
@asset(
    name="affordable_housing_production_by_building",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def affordable_housing_production_by_building(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    # downloaded from NYC Open Data on first use; see src/nycohm/fetch_data.py
    df, csv_path = _read_csv(context, ensure_dataset(DATASET))

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
        "notes": "Building-level affordable housing production. NYC Open Data export.",
    }
    return Output(df, metadata=metadata)

#process
@asset(
    ins={"affordable_housing_production_by_building": AssetIn(key=["affordable_housing_production_by_building"])},
    name="affordable_housing_production_by_building_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def affordable_housing_production_by_building_clean(affordable_housing_production_by_building) -> Output[pd.DataFrame]:
    df = affordable_housing_production_by_building
    df = standardize_null_values(df)

    # convert date columns to proper format
    df['Project_Start_Date'] = pd.to_datetime(df['Project_Start_Date'])
    df['Project_Completion_Date'] = pd.to_datetime(df['Project_Completion_Date'])

    # add dataset identifier
    df['source_dataset'] = 'Affordable Housing Units'

    # transform community board key
    MAP_BORO_CODE_2 = {
        'MN': 1,
        'BX': 2,
        'BK': 3,
        'QN': 4,
        'SI': 5
    }

    df['borough_code'] = df['Community_Board'].str[:2]
    df['borough_number'] = df['borough_code'].map(MAP_BORO_CODE_2).astype(str)
    df['board_number'] = df['Community_Board'].str[-2:].astype(str)
    df['Community_District'] = df['borough_number'] + df['board_number']

    # add key for Project level
    df['Project_Key'] = df['Project_ID'].astype(str)

    # derive the 2020 census tract GEOID: point-in-polygon on the building's
    # coordinates, falling back to decoding HPD's tract column where a row has
    # no coordinates. Rows with neither (e.g. CONFIDENTIAL projects) stay null.
    df = make_full_census_tract(
        df,
        tract_col="Census_Tract",
        borough_col="Borough",
        lat_col="Latitude",
        lon_col="Longitude",
    )
    tract_source_counts = (
        df["Census_Tract_Source"].fillna("unassigned").value_counts().to_dict()
    )
    logging.info("Census tract assignment by source: %s", tract_source_counts)
    df["Census_Tract"] = df["Census_Tract_Full"]
    df = df.drop(columns=['Census_Tract_Full', 'Census_Tract_Source'])

    # add standardized geographic key column names and correct formats
    df['Council_District'] = df['Council_District'].astype('Int64').astype(str)
    df['Community_District'] = df['Community_District'].astype('Int64').astype(str)
    # nullable string dtype so rows without a tract stay null (not the text '<NA>')
    df['Census_Tract'] = df['Census_Tract'].astype('string')
    df['BBL'] = df['BBL'].astype('Int64')

    # add shared metric columns
    df['Housing_Units'] = df['All_Counted_Units'].astype('Int64')

    # add shared filter columns
    df['Delivery_Status'] = df['Project_Completion_Date'].apply(
        lambda status: 'In Progress' if pd.isna(status) else 'Delivered'
    )

    df['Unit_Type'] = df['Reporting_Construction_Type'].apply(
        lambda status: 'New Units' if status == 'New Construction' else 'Preserved Units'
    )

    df['Project_Start_Year'] = df['Project_Start_Date'].dt.year.astype('Int64')
    df['Project_Completion_Year'] = df['Project_Completion_Date'].dt.year.astype('Int64')

    metadata = {
        "rows": len(df),
        "census_tract_source_counts": tract_source_counts,
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_affordable_housing_production_by_building = [affordable_housing_production_by_building,
                                                    affordable_housing_production_by_building_clean]