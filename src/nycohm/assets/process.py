import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.handle_null import standardize_null_values

configure_logging()

# Process transit dataset
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

    # add standardized geographic key column names
    df['Community_District'] = df['CommntyDst'].astype('Int64')
    df['Council_District'] = df['CouncilDst'].astype('Int64')
    df['Census_Tract'] = df['CenTract20'].astype(str)
    MAP_BORO_CODE_1 = {
        1: 'Manhattan',
        2: 'Bronx',
        3: 'Brooklyn',
        4: 'Queens',
        5: 'Staten Island'
    }
    df['Borough'] = df['Boro'].map(MAP_BORO_CODE_1)

    # add shared metric columns
    df['Housing_Units'] = df['ClassANet']

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
    df = standardize_null_values(df)

    # correct key formats
    df['BBL'] = df['BBL'].astype('Int64')

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
    df['board_number'] = df['Community_Board'].str[-2:]
    df['Community_District'] = df['borough_code'].map(MAP_BORO_CODE_2).astype('Int64') + df['board_number'].astype('Int64')

    # add standardized geographic key column names
    # df['Community_District'] = df['Community_Board']
    df['Council_District'] = df['Council_District'].astype('Int64')
    df['Census_Tract'] = df['Census_Tract'].astype(str)

    # add shared metric columns
    df['Housing_Units'] = df['All_Counted_Units']

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
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# Create final joined dataset
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"]),
         "new_york_36_transit_census_tract_clean": AssetIn(key=["new_york_36_transit_census_tract_clean"])},
    name="main",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def main(housingdb_post2010_clean,affordable_housing_production_by_building_clean,
         new_york_36_transit_census_tract_clean) -> Output[pd.DataFrame]:
    # get only needed columns from each dataset
    shared_columns = ['Community_District','Council_District','Census_Tract','source_dataset','Delivery_Status',
                      'Unit_Type','Housing_Units','Project_Start_Year',
                      'Project_Completion_Year','Borough','_ingested_at']
    housingdb_post2010_clean = housingdb_post2010_clean[shared_columns]
    affordable_housing_production_by_building_clean = affordable_housing_production_by_building_clean[shared_columns
    ]

    # union of housing with affordable
    df = pd.concat([housingdb_post2010_clean, affordable_housing_production_by_building_clean], ignore_index=True)

    logging.info('rows prior to transit merge: {}'.format(len(df)))

    # join transit
    df = df.merge(new_york_36_transit_census_tract_clean,left_on='Census_Tract',
                  right_on='Census_Tract',how='left')

    logging.info('rows after transit merge: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# keep this list updated
assets_process = [new_york_36_transit_census_tract_clean,housingdb_post2010_clean,
                  affordable_housing_production_by_building_clean, main]