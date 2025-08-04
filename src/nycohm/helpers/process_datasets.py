from google.cloud import bigquery
import os
from log_config import configure_logging
import logging
import pandas as pd
from connect_bq import connect_bq
from load_bq import load_bq

def process_housing():
    """
    Process the DataFrame, then load it.
    """

    # get housing data
    query = 'SELECT * FROM `nycohm.ingest.HousingDB_post2010`'
    df = connect_bq().query(query).to_dataframe()

    #drop nulls
    df = df.dropna(subset=['CommntyDst'])
    df = df.dropna(subset=['CouncilDst'])

    df['community_district'] = df['CommntyDst'].astype('Int64').astype(str)
    df['council_district'] = df['CouncilDst'].astype('Int64').astype(str)
    df['census_tract'] = df['CenTract20'].astype('Int64').astype(str)
    df['Year of Completion'] = pd.to_datetime(df['CompltYear'], format='%m/%d/%Y',errors='coerce').dt.year.astype('Int64')
    df['Permit Year'] = pd.to_datetime(df['PermitYear'], errors='coerce').dt.year.astype('Int64')

    logging.info(df.head(20).to_string())

    # load data
    load_bq(df, 'nycohm', 'processed', 'HousingDB_post2010')

BORO_MAP = {
    'MN': 1,
    'BX': 2,
    'BK': 3,
    'QN': 4,
    'SI': 5
}

def standardize_community_board(val: str | float | int | None):
    if pd.isna(val):
        return pd.NA
    if isinstance(val, str):
        val = val.strip()
        if '-' in val:
            prefix, dist = val.split('-', 1)
            prefix = prefix.strip()
            dist = dist.strip()
            boro = BORO_MAP.get(prefix.upper())
            if boro is None:
                return pd.NA
            try:
                return int(f"{boro}{int(dist):02d}")
            except ValueError:
                return pd.NA
        try:
            val = float(val)
        except ValueError:
            return pd.NA
    return int(val)

def process_affordable():
    """
    Process the DataFrame to extract primary keys for housing data.
    """

    # get affordable data
    query = 'SELECT * FROM `nycohm.ingest.Affordable_Housing_Production_by_Building_20250731`'
    df = connect_bq().query(query).to_dataframe()

    # drop nulls
    # df = df.dropna(subset=['CommntyDst'])
    # df = df.dropna(subset=['CouncilDst'])

    df['BBL'] = df['BBL'].astype('Int64')

    # correct key formats
    df['community_district'] = df['community_district'].apply(standardize_community_board).astype(str)
    df['council_district'] = df['council_district'].astype('Int64').astype(str)
    df['census_tract'] = df['census_tract'].astype('Int64').astype(str)

    df['Year of Completion'] = pd.to_datetime(df['Project_Completion_Date'],format='%m/%d/%Y', errors='coerce').dt.year.astype('Int64')

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

    # load data
    load_bq(df, 'nycohm', 'processed', 'Affordable_Housing_Production_by_Building_20250731')

def join_sets():
    """
    Join the processed housing and affordable datasets.
    """

    # Load processed datasets
    housing_df = connect_bq().query('SELECT * FROM `nycohm.processed.HousingDB_post2010`').to_dataframe()
    affordable_df = connect_bq().query('SELECT * FROM `nycohm.processed.Affordable_Housing_Production_by_Building_20250731`').to_dataframe()

    # Join on community district and council district
    joined_df = housing_df.merge(affordable_df, on=['community_district', 'council_district'], how='left')

    logging.info(joined_df.head(20).to_string())

    # Load the joined dataset
    load_bq(joined_df, 'nycohm', 'processed', 'nycohm_main')

# process_housing()
# process_affordable()
# join_sets()