import pandas as pd
from dagster import asset, Output, MetadataValue, AssetExecutionContext, AssetIn
import logging
from src.nycohm.helpers.log_config import configure_logging
from src.nycohm.helpers.handle_null import standardize_null_values
import numpy as np

configure_logging()

# aggregate on council district
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"]),
        "crosswalk_census_tract_to_cc_clean": AssetIn(key=["crosswalk_census_tract_to_cc_clean"]),
        "population_census_tract_clean": AssetIn(key=["population_census_tract_clean"]),
        "new_york_36_transit_census_tract_clean": AssetIn(key=["new_york_36_transit_census_tract_clean"])
         },
    name="agg_council_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_council_district(housingdb_post2010_clean,affordable_housing_production_by_building_clean,
                         crosswalk_census_tract_to_cc_clean,population_census_tract_clean,
                         new_york_36_transit_census_tract_clean) -> Output[pd.DataFrame]:

    # merge census tract with population and transit data
    cw = crosswalk_census_tract_to_cc_clean
    cw = cw.merge(population_census_tract_clean[['Census_Tract', 'P1_001N']],
                  left_on='Census_Tract', right_on='Census_Tract', how='left')
    cw = cw.merge(new_york_36_transit_census_tract_clean[['Census_Tract', 'jobs_at_60_mins_transit_time']],
                  left_on='Census_Tract', right_on='Census_Tract', how='left')

    # ensure valid int type
    cw['P1_001N'] = cw['P1_001N'].astype('Int64')

    # calculate average of jobs_at_60_mins_transit_time by council district, weighted by population
    cw['weighted_jobs'] = cw['jobs_at_60_mins_transit_time'] * cw['P1_001N']
    cw_agg = (cw.groupby('Council_District', as_index=False)
              .agg({'weighted_jobs': 'sum', 'P1_001N': 'sum'}))
    cw_agg['weighted_avg_jobs'] = cw_agg['weighted_jobs'] / cw_agg['P1_001N']
    cw_agg['weighted_avg_jobs'] = cw_agg['weighted_avg_jobs'].astype('Int64')

    # calculate total housing units by council district
    h = housingdb_post2010_clean

    # filter on completed housing units only
    h = h[h['Delivery_Status'] == 'Delivered']

    # Aggregate total housing units
    ha = (
        h.groupby(['Council_District'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Housing_Units'})
    )

    logging.info('aggregated rows: {}'.format(len(ha)))

    # calculate total affordablehousing units by council district
    a = affordable_housing_production_by_building_clean

    logging.info("Preview of affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # filter on completed housing units only
    a = a[a['Delivery_Status'] == 'Delivered']

    logging.info("Preview of filtered affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # Aggregate total affordable housing units
    aa = (
        a.groupby(['Council_District'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Affordable_Housing_Units'})
    )

    # merge the aggregated DataFrames. Note: transit/jobs data is at council district level only, not year-specific
    df = ha.merge(aa, on=['Council_District'], how='left')
    df = df.merge(cw_agg, on='Council_District', how='left')

    # Calculate percentiles with dense ranking to reduce duplicates
    df["Pctl_Total_Housing_Units"] = (
            df["Total_Housing_Units"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Total_Housing_Units"] = (
        pd.to_numeric(df["Pctl_Total_Housing_Units"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["Pctl_Total_Affordable_Housing_Units"] = (
            df["Total_Affordable_Housing_Units"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Total_Affordable_Housing_Units"] = (
        pd.to_numeric(df["Pctl_Total_Affordable_Housing_Units"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["Pctl_Weighted_Transit"] = (
            df["weighted_avg_jobs"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Weighted_Transit"] = (
        pd.to_numeric(df["Pctl_Weighted_Transit"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["PctlDiff_Housing_Units"] = df["Pctl_Total_Housing_Units"] - df["Pctl_Weighted_Transit"]
    logging.info(
        "Preview of df after calculating Pctl_Weighted_Transit:\n{}".format(df.head(70).to_string()))

    df["QuintDiff_Housing_Units"] = pd.cut(df["PctlDiff_Housing_Units"], 5, labels=False)
    df["Grade_Housing_Production"] = pd.cut(
        df["QuintDiff_Housing_Units"],
        bins=5,  # 5 quantiles (quintiles)
        labels=["F", "D", "C", "B", "A"]
    )

    df["PctlDiff_Affordable_Housing_Units"] = df["Pctl_Total_Affordable_Housing_Units"] - df["Pctl_Weighted_Transit"]
    df["QuintDiff_Affordable_Housing_Units"] = pd.cut(df["PctlDiff_Affordable_Housing_Units"], 5, labels=False)
    df["Grade_Affordable_Housing_Production"] = pd.cut(
        df["QuintDiff_Affordable_Housing_Units"],
        bins=5,  # 5 quantiles (quintiles)
        labels=["F", "D", "C", "B", "A"]
    )

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    # add prefix to columns to indicate council district level; keep Council_District the same
    df = df.add_prefix('CC_').rename(columns={'CC_Council_District': 'Council_District'})

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# aggregate on community district
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"]),
        "crosswalk_census_tract_to_cd_clean": AssetIn(key=["crosswalk_census_tract_to_cd_clean"]),
        "population_census_tract_clean": AssetIn(key=["population_census_tract_clean"]),
        "new_york_36_transit_census_tract_clean": AssetIn(key=["new_york_36_transit_census_tract_clean"])
         },
    name="agg_community_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_community_district(housingdb_post2010_clean,affordable_housing_production_by_building_clean,
                         crosswalk_census_tract_to_cd_clean,population_census_tract_clean,
                         new_york_36_transit_census_tract_clean) -> Output[pd.DataFrame]:

    # merge census tract with population and transit data
    cw = crosswalk_census_tract_to_cd_clean
    cw = cw.merge(population_census_tract_clean[['Census_Tract', 'P1_001N']],
                  left_on='Census_Tract', right_on='Census_Tract', how='left')
    cw = cw.merge(new_york_36_transit_census_tract_clean[['Census_Tract', 'jobs_at_60_mins_transit_time']],
                  left_on='Census_Tract', right_on='Census_Tract', how='left')

    # ensure valid int type
    cw['P1_001N'] = cw['P1_001N'].astype('Int64')

    # calculate average of jobs_at_60_mins_transit_time by council district, weighted by population
    cw['weighted_jobs'] = cw['jobs_at_60_mins_transit_time'] * cw['P1_001N']
    cw_agg = (cw.groupby('Community_District', as_index=False)
              .agg({'weighted_jobs': 'sum', 'P1_001N': 'sum'}))
    cw_agg['weighted_avg_jobs'] = cw_agg['weighted_jobs'] / cw_agg['P1_001N']
    cw_agg['weighted_avg_jobs'] = cw_agg['weighted_avg_jobs'].astype('Int64')

    # calculate total housing units by council district
    h = housingdb_post2010_clean

    # filter on completed housing units only
    h = h[h['Delivery_Status'] == 'Delivered']

    # Aggregate total housing units
    ha = (
        h.groupby(['Community_District'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Housing_Units'})
    )

    logging.info('aggregated rows: {}'.format(len(ha)))

    # calculate total affordable housing units by community district
    a = affordable_housing_production_by_building_clean

    logging.info("Preview of affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # filter on completed housing units only
    a = a[a['Delivery_Status'] == 'Delivered']

    logging.info("Preview of filtered affordable_housing_production_by_building_clean:\n{}".format(a.head(20).to_string()))

    # Aggregate total affordable housing units
    aa = (
        a.groupby(['Community_District'], as_index=False)['Housing_Units']
        .sum()
        .rename(columns={'Housing_Units': 'Total_Affordable_Housing_Units'})
    )

    logging.info(
        "Preview of aggregated affordable_housing_production_by_building_clean:\n{}".format(aa.head(70).to_string()))

    # merge the aggregated DataFrames. Note: transit/jobs data is at council district level only, not year-specific
    df = ha.merge(aa, on=['Community_District'], how='left')
    df = df.merge(cw_agg, on='Community_District', how='left')

    # Calculate percentiles with dense ranking to reduce duplicates
    df["Pctl_Total_Housing_Units"] = (
            df["Total_Housing_Units"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Total_Housing_Units"] = (
        pd.to_numeric(df["Pctl_Total_Housing_Units"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["Pctl_Total_Affordable_Housing_Units"] = (
            df["Total_Affordable_Housing_Units"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Total_Affordable_Housing_Units"] = (
        pd.to_numeric(df["Pctl_Total_Affordable_Housing_Units"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["Pctl_Weighted_Transit"] = (
            df["weighted_avg_jobs"]
            .rank(method="dense", pct=True) * 100
    )
    df["Pctl_Weighted_Transit"] = (
        pd.to_numeric(df["Pctl_Weighted_Transit"], errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(lambda s: s * 100 if s.max(skipna=True) is not None and s.max(skipna=True) <= 1 else s)
        .clip(0, 100)
        .round()
        .astype("Int64")
    )

    df["PctlDiff_Housing_Units"] = df["Pctl_Total_Housing_Units"] - df["Pctl_Weighted_Transit"]
    logging.info(
        "Preview of df after calculating Pctl_Weighted_Transit:\n{}".format(df.head(70).to_string()))

    df["QuintDiff_Housing_Units"] = pd.cut(df["PctlDiff_Housing_Units"], 5, labels=False)
    df["Grade_Housing_Production"] = pd.cut(
        df["QuintDiff_Housing_Units"],
        bins=5,  # 5 quantiles (quintiles)
        labels=["F", "D", "C", "B", "A"]
    )

    df["PctlDiff_Affordable_Housing_Units"] = df["Pctl_Total_Affordable_Housing_Units"] - df["Pctl_Weighted_Transit"]
    df["QuintDiff_Affordable_Housing_Units"] = pd.cut(df["PctlDiff_Affordable_Housing_Units"], 5, labels=False)
    df["Grade_Affordable_Housing_Production"] = pd.cut(
        df["QuintDiff_Affordable_Housing_Units"],
        bins=5,  # 5 quantiles (quintiles)
        labels=["F", "D", "C", "B", "A"]
    )

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    # add prefix to columns to indicate council district level; keep Council_District the same
    df = df.add_prefix('CD_').rename(columns={'CD_Community_District': 'Community_District'})

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


# aggregate on census tract
@asset(
    ins={"housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
         "affordable_housing_production_by_building_clean": AssetIn(key=["affordable_housing_production_by_building_clean"]),
         "new_york_36_transit_census_tract_clean": AssetIn(key=["new_york_36_transit_census_tract_clean"])},
    name="agg_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_census_tract(housingdb_post2010_clean,affordable_housing_production_by_building_clean,
                     new_york_36_transit_census_tract_clean) -> Output[pd.DataFrame]:
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

    # Merge transit data
    t = new_york_36_transit_census_tract_clean
    df = df.merge(t[['Census_Tract', 'jobs_at_60_mins_transit_time']], on='Census_Tract', how='left')

    logging.info('aggregated rows of merged df: {}'.format(len(df)))

    logging.info(df.head(20).to_string())

    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }

    return Output(df, metadata=metadata)


assets_geo_districts = [agg_council_district, agg_community_district, agg_census_tract]