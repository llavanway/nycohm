import pandas as pd
from dagster import asset, Output, MetadataValue, AssetIn

from src.housingmetrics_nyc.fetch_data import dataset_source_info
from src.housingmetrics_nyc.helpers.dashboard_export import (
    build_dashboard_main,
    write_dashboard_files,
)
from src.housingmetrics_nyc.helpers.log_config import configure_logging

configure_logging()


@asset(
    ins={
        "main": AssetIn(key=["main"]),
        "housingdb_post2010": AssetIn(key=["housingdb_post2010"]),
    },
    name="dashboard_main",
    io_manager_key="warehouse_io_manager",
    compute_kind="pandas",
    group_name="export",
)
def dashboard_main(main, housingdb_post2010) -> Output[pd.DataFrame]:
    """
    The table behind the Tableau dashboard. Materializing it also writes
    data/exports/main.csv and data/exports/nyc_combined.geojson, the two files
    the workbook connects to.
    """
    housing_version = housingdb_post2010["Version"].dropna().iloc[0]
    df = build_dashboard_main(
        main,
        housing_version=housing_version,
        affordable_source_info=dataset_source_info(
            "affordable_housing_production_by_building"
        ),
    )
    written = write_dashboard_files(df)

    metadata = {
        "rows": len(df),
        "main_csv": MetadataValue.path(str(written["main_csv"])),
        "boundaries_geojson": MetadataValue.path(str(written["boundaries_geojson"])),
        "boundary_counts": written["boundary_counts"],
        "housing_data_version": str(housing_version),
        "housing_data_as_of": df["Housing_Data_As_Of"].iloc[0],
        "affordable_data_as_of": df["Affordable_Data_As_Of"].iloc[0],
        "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
    }
    return Output(df, metadata=metadata)


assets_dashboard_export = [dashboard_main]
