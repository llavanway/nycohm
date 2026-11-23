"""Write the two files the Tableau dashboard reads.

    data/exports/main.csv              housing metrics, one row per district and dimension
    data/exports/nyc_combined.geojson  district boundaries

The file names never change, so after the first hookup a data refresh in
Tableau is: re-materialize the assets, then refresh the workbook's extract.
See docs/dashboard_refresh.md.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.housingmetrics_nyc.helpers.combine_geojson import combine_geojson
from src.housingmetrics_nyc.helpers.drop_inactive_jobs import capture_date_from_version

EXPORT_DIR = Path(__file__).resolve().parents[3] / "data" / "exports"
MAIN_CSV = "main.csv"
BOUNDARIES_GEOJSON = "nyc_combined.geojson"

# The dashboard's contract: column name -> type, in file order. The workbook
# refers to these names, so rename or drop one only together with the
# workbook. New columns go at the end.
DASHBOARD_COLUMNS = {
    "District_Type": "string",
    "District_Value": "string",
    "Delivery_Status": "string",
    "Unit_Type": "string",
    "Project_Start_Year": "integer",
    "Project_Completion_Year": "integer",
    "Borough": "string",
    "Housing_Units": "integer",
    "Housing_Units_Affordable": "integer",
    "Total_Housing_Units": "real",
    "Total_Affordable_Housing_Units": "real",
    "weighted_jobs": "real",
    "P1_001N": "real",
    "weighted_avg_jobs": "real",
    "Pctl_Total_Housing_Units": "real",
    "Pctl_Total_Affordable_Housing_Units": "real",
    "Pctl_Weighted_Transit": "real",
    "PctlDiff_Housing_Units": "real",
    "QuintDiff_Housing_Units": "real",
    "Grade_Housing_Production": "string",
    "PctlDiff_Affordable_Housing_Units": "real",
    "QuintDiff_Affordable_Housing_Units": "real",
    "Grade_Affordable_Housing_Production": "string",
    "District_Label": "string",
    "Rank_Total_Housing_Units": "integer",
    "Rank_Total_Affordable_Housing_Units": "integer",
    "District_Count": "integer",
    "Housing_Data_Version": "string",
    "Housing_Data_As_Of": "string",
    "Affordable_Data_As_Of": "string",
    "Council_Member": "string",
    "Neighborhoods": "string",
    "Is_Placeholder": "integer",
    "Council_Members_As_Of": "string",
}

_PANDAS_DTYPE = {"string": "string", "integer": "Int64", "real": "float64"}


def format_date(value) -> str:
    """2026-06-30 -> 'June 30, 2026'."""
    return f"{value:%B} {value.day}, {value.year}"


def housing_data_as_of(version: str) -> str:
    """The last day covered by a Housing Database release: '26Q2.1' -> 'June 30, 2026'."""
    return format_date(capture_date_from_version(version) - timedelta(days=1))


def source_data_as_of(source_info: dict) -> str:
    """
    When NYC Open Data last updated a downloaded dataset, from the .source.json
    written by fetch_data. Falls back to the download time, then to 'Unknown'
    for a file that was put in place by hand.
    """
    timestamp = source_info.get("data_updated_at") or source_info.get("downloaded_at")
    return format_date(datetime.fromisoformat(timestamp)) if timestamp else "Unknown"


def build_dashboard_main(
    main: pd.DataFrame,
    housing_version: str,
    affordable_source_info: dict,
    council_source_info: dict,
) -> pd.DataFrame:
    """Shape the main table to the dashboard's contract and stamp it with data dates."""
    df = main.copy()
    df["Housing_Data_Version"] = str(housing_version)
    df["Housing_Data_As_Of"] = housing_data_as_of(housing_version)
    df["Affordable_Data_As_Of"] = source_data_as_of(affordable_source_info)
    df["Council_Members_As_Of"] = source_data_as_of(council_source_info)

    missing = [column for column in DASHBOARD_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"main is missing dashboard columns: {missing}")

    df = df[list(DASHBOARD_COLUMNS)]
    for column, kind in DASHBOARD_COLUMNS.items():
        if kind == "string":
            # grades arrive as categoricals; nulls must stay null, not the text 'nan'
            df[column] = df[column].astype("object").astype("string")
        else:
            df[column] = pd.to_numeric(df[column], errors="raise").astype(_PANDAS_DTYPE[kind])
    return df


def write_dashboard_files(dashboard_main: pd.DataFrame, export_dir=EXPORT_DIR) -> dict:
    """Write main.csv and nyc_combined.geojson; returns what was written."""
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    main_path = export_dir / MAIN_CSV
    dashboard_main.to_csv(main_path, index=False)

    boundaries_path = export_dir / BOUNDARIES_GEOJSON
    boundary_counts = combine_geojson(boundaries_path)

    return {
        "main_csv": main_path,
        "main_rows": len(dashboard_main),
        "boundaries_geojson": boundaries_path,
        "boundary_counts": boundary_counts,
    }
