import pandas as pd
from dagster import asset, Output, MetadataValue, AssetIn

from src.nycohm.helpers.district_labels import build_district_labels
from src.nycohm.helpers.log_config import configure_logging

configure_logging()


@asset(
    ins={
        # the raw crosswalk keeps the borough, neighborhood and CDTA names
        "crosswalk_census_tract_to_cd": AssetIn(key=["crosswalk_census_tract_to_cd"]),
        "crosswalk_census_tract_to_cc_clean": AssetIn(key=["crosswalk_census_tract_to_cc_clean"]),
    },
    name="district_labels",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="process",
)
def district_labels(
    crosswalk_census_tract_to_cd, crosswalk_census_tract_to_cc_clean
) -> Output[pd.DataFrame]:
    """One readable name per council district, community district and census tract."""
    df = build_district_labels(
        crosswalk_census_tract_to_cd,
        crosswalk_census_tract_to_cc_clean["Council_District"],
    )

    metadata = {
        "rows": len(df),
        "districts_by_type": df["District_Type"].value_counts().to_dict(),
        "preview": MetadataValue.md(df.groupby("District_Type").head(3).to_markdown(index=False)),
    }
    return Output(df, metadata=metadata)


assets_district_labels = [district_labels]
