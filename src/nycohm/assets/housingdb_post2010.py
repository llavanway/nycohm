import os
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

ingest_path = Path(__file__).resolve().parents[3] / 'data/raw_csv/housingdb_post2010.csv'


# ingest
@asset(
    name="housingdb_post2010",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="ingest_csv",
)
def housingdb_post2010(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    df, csv_path = _read_csv(context, ingest_path)

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


# Process
@asset(
    ins={"housingdb_post2010": AssetIn(key=["housingdb_post2010"])},
    name="housingdb_post2010_clean",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def housingdb_post2010_clean(housingdb_post2010) -> Output[pd.DataFrame]:
    df = housingdb_post2010
    df = standardize_null_values(df)

    # drop nulls
    df = df.dropna(subset=['CommntyDst'])
    df = df.dropna(subset=['CouncilDst'])

    # adjust types
    df['CommntyDst'] = df['CommntyDst'].astype('Int64').astype(str)
    df['CouncilDst'] = df['CouncilDst'].astype('Int64').astype(str)
    df['CenTract20'] = df['CenTract20'].astype('Int64').astype(str)
    df['CompltYear'] = df['CompltYear'].astype('Int64')
    df['PermitYear'] = df['PermitYear'].astype('Int64')

    # add dataset identifier
    df['source_dataset'] = 'Housing Units'

    # add standardized geographic key column names and formats
    df['Community_District'] = df['CommntyDst'].astype('Int64').astype(str)
    df['Council_District'] = df['CouncilDst'].astype('Int64').astype(str)
    df['Census_Tract'] = df['CenTract20'].astype(str)
    MAP_BORO_CODE_1 = {
        1: 'Manhattan',
        2: 'Bronx',
        3: 'Brooklyn',
        4: 'Queens',
        5: 'Staten Island'
    }
    df['Borough'] = df['Boro'].map(MAP_BORO_CODE_1)

    # add key for Project level
    df['Project_Key'] = df['Job_Number'].astype(str)

    # add shared metric columns
    df['Housing_Units'] = df['ClassANet'].astype('Int64')

    # add shared filter columns
    df['Delivery_Status'] = df['Job_Status'].apply(
        lambda status: 'Delivered' if status == '5. Completed Construction' else 'In Progress'
    )

    df['Unit_Type'] = df['Job_Type'].apply(
        lambda status: 'New Units' if status == 'New Building' else 'Preserved Units'
    )

    df['DateFiled'] = pd.to_datetime(df['DateFiled'], errors='coerce')
    df['Project_Start_Year'] = df['DateFiled'].dt.year.astype('Int64')
    df['Project_Completion_Year'] = df['CompltYear'].astype('Int64')

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_housingdb_post2010 = [housingdb_post2010,housingdb_post2010_clean]
