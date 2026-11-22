import json

import pandas as pd

from src.housingmetrics_nyc.helpers.combine_geojson import combine_geojson
from src.housingmetrics_nyc.helpers.dashboard_export import (
    DASHBOARD_COLUMNS,
    source_data_as_of,
    build_dashboard_main,
    housing_data_as_of,
)


def _main_row(**overrides):
    row = {column: None for column in DASHBOARD_COLUMNS}
    row.update(
        District_Type="Council_District",
        District_Value="33",
        Delivery_Status="Delivered",
        Unit_Type="New Units",
        Project_Start_Year=2019,
        Project_Completion_Year=2022,
        Borough="Brooklyn",
        Housing_Units=120,
        Pctl_Weighted_Transit=86,
        Grade_Housing_Production="B",
        District_Label="Council District 33 (Lincoln Restler)",
        Council_Member="Lincoln Restler",
        Neighborhoods="Greenpoint, Williamsburg",
        Is_Placeholder=0,
        Rank_Total_Housing_Units=1,
        Rank_Total_Affordable_Housing_Units=14,
        District_Count=51,
    )
    row.update(overrides)
    return row


def test_data_dates():
    assert housing_data_as_of("26Q2.1") == "June 30, 2026"
    assert housing_data_as_of("24Q4") == "December 31, 2024"
    assert source_data_as_of({"data_updated_at": "2026-09-30T19:21:51+00:00"}) == "September 30, 2026"
    # older downloads only recorded when they were fetched
    assert source_data_as_of({"downloaded_at": "2026-11-02T08:00:00+00:00"}) == "November 2, 2026"
    assert source_data_as_of({}) == "Unknown"


def test_dashboard_main_follows_the_contract():
    main = pd.DataFrame([_main_row(), _main_row(Project_Completion_Year=None, Housing_Units=None)])
    # pipeline columns arrive in a different order, with extras and categoricals
    main = main.drop(
        columns=["Housing_Data_Version", "Housing_Data_As_Of", "Affordable_Data_As_Of", "Council_Members_As_Of"]
    )
    main["Grade_Housing_Production"] = main["Grade_Housing_Production"].astype("category")
    main["not_for_the_dashboard"] = 1
    main = main[sorted(main.columns)]

    out = build_dashboard_main(
        main,
        "26Q2.1",
        affordable_source_info={"data_updated_at": "2026-09-30T19:21:51+00:00"},
        council_source_info={"data_updated_at": "2026-04-10T15:00:00+00:00"},
    )

    assert list(out.columns) == list(DASHBOARD_COLUMNS)
    assert out["Housing_Data_Version"].tolist() == ["26Q2.1"] * 2
    assert out["Housing_Data_As_Of"].tolist() == ["June 30, 2026"] * 2
    assert out["Affordable_Data_As_Of"].tolist() == ["September 30, 2026"] * 2
    assert out["Council_Members_As_Of"].tolist() == ["April 10, 2026"] * 2

    import csv as csv_module, io

    first, second = csv_module.DictReader(io.StringIO(out.to_csv(index=False)))
    assert list(first)[:2] == ["District_Type", "District_Value"]
    # integers stay whole numbers, reals keep a decimal, nulls are empty
    assert first["Project_Completion_Year"] == "2022" and first["Housing_Units"] == "120"
    assert first["Pctl_Weighted_Transit"] == "86.0"
    assert first["Rank_Total_Housing_Units"] == "1" and first["District_Count"] == "51"
    assert first["Is_Placeholder"] == "0" and first["Neighborhoods"] == "Greenpoint, Williamsburg"
    assert second["Project_Completion_Year"] == "" and second["Housing_Units"] == ""
    assert first["Grade_Housing_Production"] == "B" and first["Grade_Affordable_Housing_Production"] == ""


def test_combined_boundaries(tmp_path):
    counts = combine_geojson(tmp_path / "combined.geojson")
    assert counts == {"Council_District": 51, "Community_District": 71, "Census_Tract": 2325}

    features = json.loads((tmp_path / "combined.geojson").read_text())["features"]
    assert len(features) == 51 + 71 + 2325
    assert all(set(f["properties"]) == {"district_type", "district_id"} for f in features)
    ids = {(f["properties"]["district_type"], f["properties"]["district_id"]) for f in features}
    assert len(ids) == len(features)
    assert {("Council_District", "33"), ("Community_District", "301"), ("Census_Tract", "36047055500")} <= ids

    # reprojected from state plane feet to longitude/latitude around New York City
    def first_point(coordinates):
        while isinstance(coordinates[0], list):
            coordinates = coordinates[0]
        return coordinates

    for feature in features[::100]:
        lon, lat = first_point(feature["geometry"]["coordinates"])
        assert -74.3 < lon < -73.6 and 40.4 < lat < 41.0
