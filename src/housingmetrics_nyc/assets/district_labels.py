import pandas as pd
from dagster import asset, Output, MetadataValue, AssetIn

from src.housingmetrics_nyc.helpers.district_labels import build_district_labels
from src.housingmetrics_nyc.helpers.log_config import configure_logging

configure_logging()


@asset(
    ins={
        # the raw crosswalk keeps the borough, neighborhood and CDTA names
        "crosswalk_census_tract_to_cd": AssetIn(key=["crosswalk_census_tract_to_cd"]),
        "crosswalk_census_tract_to_cc_clean": AssetIn(key=["crosswalk_census_tract_to_cc_clean"]),
        "population_census_tract_clean": AssetIn(key=["population_census_tract_clean"]),
        "council_members_clean": AssetIn(key=["council_members_clean"]),
    },
    name="district_labels",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def district_labels(
    crosswalk_census_tract_to_cd,
    crosswalk_census_tract_to_cc_clean,
    population_census_tract_clean,
    council_members_clean,
) -> Output[pd.DataFrame]:
    """Name, Council Member and neighborhoods for every council district,
    community district and census tract."""
    df = build_district_labels(
        tract_crosswalk=crosswalk_census_tract_to_cd,
        tract_to_council=crosswalk_census_tract_to_cc_clean,
        population=population_census_tract_clean,
        council_members=council_members_clean,
    )

    metadata = {
        "rows": len(df),
        "districts_by_type": df["District_Type"].value_counts().to_dict(),
        "residential_by_type": df[df["Residential"]]["District_Type"].value_counts().to_dict(),
        "preview": MetadataValue.md(df.groupby("District_Type").head(3).to_markdown(index=False)),
    }
    return Output(df, metadata=metadata)


assets_district_labels = [district_labels]
