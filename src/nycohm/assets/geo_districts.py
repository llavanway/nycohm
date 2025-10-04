import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.handle_null import standardize_null_values

configure_logging()

# aggregate on council district
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"])},
    name="agg_council_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_council_district(housingdb_post2010_clean,affordable_housing_production_by_building_clean) -> Output[pd.DataFrame]:
    h = housingdb_post2010_clean

    # filter on completed housing units only
    h = h[h['Delivery_Status'] == 'Delivered']

    # Aggregate total housing units
    ha = (
        h.groupby(['Council_District', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Housing_Units'})
    )

    logging.info('aggregated rows: {}'.format(len(ha)))

    a = affordable_housing_production_by_building_clean

    logging.info("Preview of affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # filter on completed housing units only
    a = a[a['Delivery_Status'] == 'Delivered']

    logging.info("Preview of filtered affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # Aggregate total affordable housing units
    aa = (
        a.groupby(['Council_District', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Affordable_Housing_Units'})
    )

    # Merge the two aggregated DataFrames on Council_District and Project_Completion_Year
    df = ha.merge(aa, on=['Council_District', 'Project_Completion_Year'], how='left')

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)

assets_geo_districts = [agg_council_district]


# aggregate on community district
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"])},
    name="agg_community_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_community_district(housingdb_post2010_clean,affordable_housing_production_by_building_clean) -> Output[pd.DataFrame]:
    h = housingdb_post2010_clean

    # filter on completed housing units only
    h = h[h['Delivery_Status'] == 'Delivered']

    # Aggregate total housing units
    ha = (
        h.groupby(['Community_District', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Housing_Units'})
    )

    logging.info('aggregated rows: {}'.format(len(ha)))

    a = affordable_housing_production_by_building_clean

    logging.info("Preview of affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # filter on completed housing units only
    a = a[a['Delivery_Status'] == 'Delivered']

    logging.info("Preview of filtered affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # Aggregate total affordable housing units
    aa = (
        a.groupby(['Community_District', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Affordable_Housing_Units'})
    )

    # Merge the two aggregated DataFrames on Council_District and Project_Completion_Year
    df = ha.merge(aa, on=['Community_District', 'Project_Completion_Year'], how='left')

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# aggregate on census tract
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"])},
    name="agg_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_census_tract(housingdb_post2010_clean,affordable_housing_production_by_building_clean) -> Output[pd.DataFrame]:
    h = housingdb_post2010_clean

    # filter on completed housing units only
    h = h[h['Delivery_Status'] == 'Delivered']

    # Aggregate total housing units
    ha = (
        h.groupby(['Census_Tract', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Housing_Units'})
    )

    logging.info('aggregated rows: {}'.format(len(ha)))

    a = affordable_housing_production_by_building_clean

    logging.info("Preview of affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # filter on completed housing units only
    a = a[a['Delivery_Status'] == 'Delivered']

    logging.info("Preview of filtered affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # Aggregate total affordable housing units
    aa = (
        a.groupby(['Census_Tract', 'Project_Completion_Year'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Affordable_Housing_Units'})
    )

    # Merge the two aggregated DataFrames on Council_District and Project_Completion_Year
    df = ha.merge(aa, on=['Census_Tract', 'Project_Completion_Year'], how='left')

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_geo_districts = [agg_council_district, agg_community_district, agg_census_tract]