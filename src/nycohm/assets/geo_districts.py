import logging
from typing import Dict

import numpy as np
import pandas as pd
from dagster import AssetIn, MetadataValue, Output, asset

from src.nycohm.helpers.log_config import configure_logging

configure_logging()

LEVEL_PREFIXES: Dict[str, str] = {
    "Council_District": "CC_",
    "Community_District": "CD_",
    "Census_Tract": "CT_",
}


def _prepare_weighted_transit(
    crosswalk_df: pd.DataFrame,
    population_df: pd.DataFrame,
    transit_df: pd.DataFrame,
    level_col: str,
) -> pd.DataFrame:
    """Prepare weighted transit accessibility metrics for the requested level."""
    cw = crosswalk_df.copy()
    cw = cw.merge(
        population_df[["Census_Tract", "P1_001N"]],
        on="Census_Tract",
        how="left",
    )
    cw = cw.merge(
        transit_df[["Census_Tract", "jobs_at_60_mins_transit_time"]],
        on="Census_Tract",
        how="left",
    )
    cw["P1_001N"] = cw["P1_001N"].astype("Int64")

    cw["weighted_jobs"] = cw["jobs_at_60_mins_transit_time"] * cw["P1_001N"]

    grouped = (
        cw.groupby(level_col, as_index=False)
        .agg({"weighted_jobs": "sum", "P1_001N": "sum"})
    )
    grouped["weighted_avg_jobs"] = grouped["weighted_jobs"] / grouped["P1_001N"]
    grouped["weighted_avg_jobs"] = grouped["weighted_avg_jobs"].astype("Int64")

    return grouped


def _aggregate_units(
    df: pd.DataFrame,
    level_col: str,
    result_col: str,
    source_name: str,
) -> pd.DataFrame:
    """Aggregate delivered housing units for the requested level."""
    logging.info("Preview of %s:\n%s", source_name, df.head(20).to_string())
    filtered = df[df["Delivery_Status"] == "Delivered"]
    logging.info(
        "Preview of filtered %s:\n%s",
        source_name,
        filtered.head(20).to_string(),
    )
    aggregated = (
        filtered.groupby(level_col, as_index=False)["Housing_Units"]
        .sum()
        .rename(columns={"Housing_Units": result_col})
    )
    logging.info(
        "Aggregated rows for %s at %s: %s",
        source_name,
        level_col,
        len(aggregated),
    )
    return aggregated


def _compute_percentile(series: pd.Series) -> pd.Series:
    percentile = series.rank(method="dense", pct=True) * 100
    percentile = (
        pd.to_numeric(percentile, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
        .pipe(
            lambda s: s * 100
            if s.max(skipna=True) is not None and s.max(skipna=True) <= 1
            else s
        )
        .clip(0, 100)
        .round()
        .astype("Int64")
    )
    return percentile


def _add_percentiles_and_grades(df: pd.DataFrame) -> pd.DataFrame:
    df["Pctl_Total_Housing_Units"] = _compute_percentile(df["Total_Housing_Units"])
    df["Pctl_Total_Affordable_Housing_Units"] = _compute_percentile(
        df["Total_Affordable_Housing_Units"]
    )
    df["Pctl_Weighted_Transit"] = _compute_percentile(df["weighted_avg_jobs"])

    df["PctlDiff_Housing_Units"] = (
        df["Pctl_Total_Housing_Units"] - df["Pctl_Weighted_Transit"]
    )
    df["QuintDiff_Housing_Units"] = pd.cut(
        df["PctlDiff_Housing_Units"], 5, labels=False
    )
    df["Grade_Housing_Production"] = pd.cut(
        df["QuintDiff_Housing_Units"],
        bins=5,
        labels=["F", "D", "C", "B", "A"],
    )

    df["PctlDiff_Affordable_Housing_Units"] = (
        df["Pctl_Total_Affordable_Housing_Units"] - df["Pctl_Weighted_Transit"]
    )
    df["QuintDiff_Affordable_Housing_Units"] = pd.cut(
        df["PctlDiff_Affordable_Housing_Units"], 5, labels=False
    )
    df["Grade_Affordable_Housing_Production"] = pd.cut(
        df["QuintDiff_Affordable_Housing_Units"],
        bins=5,
        labels=["F", "D", "C", "B", "A"],
    )
    return df


def _create_output(df: pd.DataFrame) -> Output[pd.DataFrame]:
    metadata = {
        "rows": len(df),
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }
    return Output(df, metadata=metadata)

def _aggregate_housing_by_dims(
    level: str, housing_df: pd.DataFrame, affordable_df: pd.DataFrame
) -> pd.DataFrame:
    """Aggregate housing units by the requested level while preserving specified dimensions
    and include a Housing_Units_Affordable column from the affordable dataset."""
    logging.info("Aggregating housing metrics for %s preserving dimensions", level)
    group_cols = [
        level,
        "Delivery_Status",
        "Unit_Type",
        "Project_Start_Year",
        "Project_Completion_Year",
        "Borough",
    ]

    h_df = housing_df.copy()
    a_df = affordable_df.copy()

    aggregated = (
        # Include rows where grouping columns are null
        h_df.groupby(group_cols, as_index=False, dropna=False)["Housing_Units"]
        .sum()
        .astype({"Housing_Units": "Int64"})
    )

    aggregated_affordable = (
        # Include rows where grouping columns are null
        a_df.groupby(group_cols, as_index=False, dropna=False)["Housing_Units"]
        .sum()
        .rename(columns={"Housing_Units": "Housing_Units_Affordable"})
        .astype({"Housing_Units_Affordable": "Int64"})
    )

    # Merge so we retain groupings present in either dataset
    merged = aggregated.merge(aggregated_affordable, on=group_cols, how="outer")

    logging.info(
        "Aggregated housing metrics rows for %s at dims %s: %s",
        level,
        group_cols[1:],
        len(merged),
    )
    return merged


def aggregate_geo_level(
    level: str,
    housing_df: pd.DataFrame,
    affordable_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    population_df: pd.DataFrame,
    transit_df: pd.DataFrame,
) -> pd.DataFrame:
    logging.info("Starting aggregation for %s", level)
    weighted_transit = _prepare_weighted_transit(
        crosswalk_df,
        population_df,
        transit_df,
        level,
    )

    housing_agg = _aggregate_units(
        housing_df,
        level,
        "Total_Housing_Units",
        "housingdb_post2010_clean",
    )
    affordable_agg = _aggregate_units(
        affordable_df,
        level,
        "Total_Affordable_Housing_Units",
        "affordable_housing_production_by_building_clean",
    )

    # Use an outer merge so all districts from any source are retained,
    # then ensure unit columns exist and fill missing values with 0.
    merged = (
        housing_agg.merge(affordable_agg, on=level, how="outer")
        .merge(weighted_transit, on=level, how="outer")
    )

    # Ensure the expected unit columns exist
    for col in ["Total_Housing_Units", "Total_Affordable_Housing_Units"]:
        if col not in merged.columns:
            merged[col] = pd.Series(dtype="Int64")

    merged[["Total_Housing_Units", "Total_Affordable_Housing_Units"]] = (
        merged[["Total_Housing_Units", "Total_Affordable_Housing_Units"]]
        .fillna(0)
        .astype("Int64")
    )

    logging.info(
        "Preview of merged dataframe for %s:\n%s",
        level,
        merged.head(70).to_string(),
    )

    merged = _add_percentiles_and_grades(merged)

    logging.info(
        "Preview after percentile calculations for %s:\n%s",
        level,
        merged.head(70).to_string(),
    )

    prefix = LEVEL_PREFIXES[level]
    merged = merged.add_prefix(prefix).rename(
        columns={f"{prefix}{level}": level}
    )

    logging.info(merged.head(20).to_string())

    return merged


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        "crosswalk_census_tract_to_cc_clean": AssetIn(
            key=["crosswalk_census_tract_to_cc_clean"]
        ),
        "population_census_tract_clean": AssetIn(
            key=["population_census_tract_clean"]
        ),
        "new_york_36_transit_census_tract_clean": AssetIn(
            key=["new_york_36_transit_census_tract_clean"]
        ),
    },
    name="agg_council_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_council_district(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cc_clean,
    population_census_tract_clean,
    new_york_36_transit_census_tract_clean,
) -> Output[pd.DataFrame]:
    df = aggregate_geo_level(
        level="Council_District",
        housing_df=housingdb_post2010_clean,
        affordable_df=affordable_housing_production_by_building_clean,
        crosswalk_df=crosswalk_census_tract_to_cc_clean,
        population_df=population_census_tract_clean,
        transit_df=new_york_36_transit_census_tract_clean,
    )
    return _create_output(df)


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        "crosswalk_census_tract_to_cd_clean": AssetIn(
            key=["crosswalk_census_tract_to_cd_clean"]
        ),
        "population_census_tract_clean": AssetIn(
            key=["population_census_tract_clean"]
        ),
        "new_york_36_transit_census_tract_clean": AssetIn(
            key=["new_york_36_transit_census_tract_clean"]
        ),
    },
    name="agg_community_district",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_community_district(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cd_clean,
    population_census_tract_clean,
    new_york_36_transit_census_tract_clean,
) -> Output[pd.DataFrame]:
    df = aggregate_geo_level(
        level="Community_District",
        housing_df=housingdb_post2010_clean,
        affordable_df=affordable_housing_production_by_building_clean,
        crosswalk_df=crosswalk_census_tract_to_cd_clean,
        population_df=population_census_tract_clean,
        transit_df=new_york_36_transit_census_tract_clean,
    )
    return _create_output(df)


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        "crosswalk_census_tract_to_cd_clean": AssetIn(
            key=["crosswalk_census_tract_to_cd_clean"]
        ),
        "population_census_tract_clean": AssetIn(
            key=["population_census_tract_clean"]
        ),
        "new_york_36_transit_census_tract_clean": AssetIn(
            key=["new_york_36_transit_census_tract_clean"]
        ),
    },
    name="agg_census_tract",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_census_tract(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cd_clean,
    population_census_tract_clean,
    new_york_36_transit_census_tract_clean,
) -> Output[pd.DataFrame]:
    df = aggregate_geo_level(
        level="Census_Tract",
        housing_df=housingdb_post2010_clean,
        affordable_df=affordable_housing_production_by_building_clean,
        crosswalk_df=crosswalk_census_tract_to_cd_clean,
        population_df=population_census_tract_clean,
        transit_df=new_york_36_transit_census_tract_clean,
    )
    return _create_output(df)


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        "crosswalk_census_tract_to_cc_clean": AssetIn(
            key=["crosswalk_census_tract_to_cc_clean"]
        ),
    },
    name="agg_housing_metrics_cc",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_housing_metrics_cc(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cc_clean,  # kept in inputs for consistency; not required here
) -> Output[pd.DataFrame]:
    df = _aggregate_housing_by_dims(
        "Council_District", housingdb_post2010_clean, affordable_housing_production_by_building_clean
    )
    return _create_output(df)


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        "crosswalk_census_tract_to_cd_clean": AssetIn(
            key=["crosswalk_census_tract_to_cd_clean"]
        ),
    },
    name="agg_housing_metrics_cd",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_housing_metrics_cd(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cd_clean,  # kept in inputs for consistency; not required here
) -> Output[pd.DataFrame]:
    df = _aggregate_housing_by_dims(
        "Community_District", housingdb_post2010_clean, affordable_housing_production_by_building_clean
    )
    return _create_output(df)


@asset(
    ins={
        "housingdb_post2010_clean": AssetIn(key=["housingdb_post2010_clean"]),
        "affordable_housing_production_by_building_clean": AssetIn(
            key=["affordable_housing_production_by_building_clean"]
        ),
        # a crosswalk input is optional for census tract; included for symmetry with other assets
        "crosswalk_census_tract_to_cd_clean": AssetIn(
            key=["crosswalk_census_tract_to_cd_clean"]
        ),
    },
    name="agg_housing_metrics_ct",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def agg_housing_metrics_ct(
    housingdb_post2010_clean,
    affordable_housing_production_by_building_clean,
    crosswalk_census_tract_to_cd_clean,
) -> Output[pd.DataFrame]:
    df = _aggregate_housing_by_dims(
        "Census_Tract", housingdb_post2010_clean, affordable_housing_production_by_building_clean
    )
    return _create_output(df)


assets_geo_districts = [
    agg_council_district,
    agg_community_district,
    agg_census_tract,
    agg_housing_metrics_cc,
    agg_housing_metrics_cd,
    agg_housing_metrics_ct
]
