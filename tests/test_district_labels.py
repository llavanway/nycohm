from pathlib import Path

import pandas as pd
import pytest

from src.housingmetrics_nyc.helpers.district_labels import (
    UNIT_TYPES,
    UNASSIGNED_LABEL,
    add_district_labels,
    build_district_labels,
    placeholder_rows,
    placeholder_years,
)

DATA = Path(__file__).resolve().parents[1] / "data"


@pytest.fixture(scope="module")
def districts():
    # default dtypes, as the ingest assets read them: GEOID and BoroCode become integers
    crosswalk = pd.read_csv(DATA / "crosswalk/nyc_2020_census_tract_nta_cdta_relationships.csv")
    council = pd.read_csv(DATA / "crosswalk/nyc_2020_census_tract_ccd_2023_relationships.csv")
    tract_to_council = pd.DataFrame(
        {"Census_Tract": council["GEOID"].astype(str), "Council_District": council["CCD2023"].astype(str)}
    )
    census = pd.read_csv(DATA / "raw_csv/DECENNIALPL2020.P1-Data.csv", skiprows=[1], usecols=["GEO_ID", "P1_001N"])
    population = pd.DataFrame(
        {"Census_Tract": census["GEO_ID"].str[-11:], "P1_001N": pd.to_numeric(census["P1_001N"], errors="coerce")}
    )
    members = pd.DataFrame(
        # district 3 has no sitting member in this fixture
        {"Council_District": [str(n) for n in range(1, 52) if n != 3],
         "Council_Member": [f"Member {n}" for n in range(1, 52) if n != 3]}
    )
    return build_district_labels(crosswalk, tract_to_council, population, members)


def _row(districts, district_type, value):
    match = districts[(districts["District_Type"] == district_type) & (districts["District_Value"] == value)]
    assert len(match) == 1
    return match.iloc[0]


def test_one_unique_label_per_district(districts):
    assert districts["District_Type"].value_counts().to_dict() == {
        "Census_Tract": 2327,
        "Community_District": 71,
        "Council_District": 51,
    }
    assert districts["District_Label"].is_unique
    assert not districts.duplicated(subset=["District_Type", "District_Value"]).any()


@pytest.mark.parametrize(
    "district_type, value, label, neighborhoods",
    [
        ("Community_District", "301", "Brooklyn Community District 1", "Williamsburg-Greenpoint"),
        ("Community_District", "112", "Manhattan Community District 12", "Washington Heights-Inwood"),
        # joint interest areas (parks, airports) are not community districts
        ("Community_District", "164", "Manhattan Joint Interest Area 64", "Central Park"),
        # Rikers Island is a Bronx tract in Queens CD 1; it must not rename Bronx CD 1
        ("Community_District", "201", "Bronx Community District 1", "Melrose-Mott Haven-Port Morris"),
        ("Community_District", "401", "Queens Community District 1", "Astoria-Queensbridge"),
        ("Census_Tract", "36005001901", "Bronx Census Tract 19.01", "Mott Haven-Port Morris"),
        ("Census_Tract", "36005002300", "Bronx Census Tract 23", "Mott Haven-Port Morris"),
    ],
)
def test_label_and_neighborhood_formats(districts, district_type, value, label, neighborhoods):
    row = _row(districts, district_type, value)
    assert row["District_Label"] == label
    assert row["Neighborhoods"] == neighborhoods
    assert pd.isna(row["Council_Member"])


def test_council_districts_carry_the_member_and_neighborhoods(districts):
    row = _row(districts, "Council_District", "33")
    assert row["District_Label"] == "Council District 33 (Member 33)"
    assert row["Council_Member"] == "Member 33"
    # the largest residential neighborhoods by population, biggest first, at most four
    names = row["Neighborhoods"].split(", ")
    assert names[0] == "Greenpoint" and 2 <= len(names) <= 4
    assert "Brooklyn Heights" in names

    vacant = _row(districts, "Council_District", "3")
    assert vacant["District_Label"] == "Council District 3 (no member listed)"
    assert pd.isna(vacant["Council_Member"])

    council = districts[districts["District_Type"] == "Council_District"]
    assert council["Neighborhoods"].notna().all()
    assert council["Neighborhoods"].str.count(", ").add(1).between(1, 4).all()


def test_residential_flag(districts):
    council = districts[districts["District_Type"] == "Council_District"]
    community = districts[districts["District_Type"] == "Community_District"]
    assert council["Residential"].all()
    assert int(community["Residential"].sum()) == 59  # the 59 community districts, not the parks and airports
    assert not _row(districts, "Community_District", "164")["Residential"]  # Central Park
    assert _row(districts, "Census_Tract", "36005001901")["Residential"]
    assert not _row(districts, "Census_Tract", "36061014300")["Residential"]  # Central Park's tract


def test_add_district_labels_fallbacks(districts):
    df = pd.DataFrame(
        {
            "District_Type": ["Council_District", "Census_Tract", "Census_Tract", "Community_District"],
            "District_Value": ["33", "99999999999", None, "<NA>"],
            "Housing_Units": [1, 2, 3, 4],
        }
    )
    out = add_district_labels(df, districts)
    assert out["District_Label"].tolist() == [
        "Council District 33 (Member 33)",
        "Census Tract 99999999999",  # not in the crosswalk
        UNASSIGNED_LABEL,
        UNASSIGNED_LABEL,
    ]
    assert out["Neighborhoods"].notna().tolist() == [True, False, False, False]
    assert out["Housing_Units"].tolist() == [1, 2, 3, 4]


START_YEARS = [1998, 2000, 2009, 2010, 2011, 2012]
COMPLETION_YEARS = [2010, 2011, 2012, 2013]


def test_placeholder_years_cover_every_year_of_both_filters():
    years = placeholder_years(START_YEARS, COMPLETION_YEARS)
    delivered = years[years["Delivery_Status"] == "Delivered"]
    in_progress = years[years["Delivery_Status"] == "In Progress"]

    # filtering to any one start year, or any one completion year, leaves a row
    assert set(delivered["Project_Start_Year"]) == set(START_YEARS)
    assert set(delivered["Project_Completion_Year"]) == set(COMPLETION_YEARS)
    assert set(in_progress["Project_Start_Year"]) == set(START_YEARS)
    assert in_progress["Project_Completion_Year"].isna().all()

    # no impossible rows, and no years the data does not already have
    assert (delivered["Project_Start_Year"] <= delivered["Project_Completion_Year"]).all()
    pairs = set(zip(delivered["Project_Start_Year"], delivered["Project_Completion_Year"]))
    assert (1998, 2010) in pairs and (2011, 2011) in pairs
    assert (2012, 2013) in pairs  # 2013 has no matching start year, so it pairs with the latest one
    assert not years.duplicated().any()


def test_placeholder_years_without_any_years():
    years = placeholder_years([], [])
    assert years["Delivery_Status"].tolist() == ["Delivered", "In Progress"]
    assert years["Project_Start_Year"].isna().all() and years["Project_Completion_Year"].isna().all()


def test_placeholder_rows_cover_residential_districts_only(districts):
    rows = placeholder_rows(districts, START_YEARS, COMPLETION_YEARS)
    residential = districts[districts["Residential"]]
    per_district = len(placeholder_years(START_YEARS, COMPLETION_YEARS)) * len(UNIT_TYPES)
    assert len(rows) == len(residential) * per_district
    assert (rows["Housing_Units"] == 0).all() and (rows["Housing_Units_Affordable"] == 0).all()
    assert (rows["Is_Placeholder"] == 1).all()
    assert rows["Project_Start_Year"].notna().all()

    keys = set(zip(rows["District_Type"], rows["District_Value"]))
    assert ("Community_District", "164") not in keys  # Central Park stays blank
    assert ("Council_District", "3") in keys

    one = rows[(rows["District_Type"] == "Census_Tract") & (rows["District_Value"] == "36047055500")]
    assert len(one) == per_district
    assert set(one["Unit_Type"]) == set(UNIT_TYPES)
    # every district is still there after filtering to a single year
    for unit_type in UNIT_TYPES:
        delivered = one[(one["Unit_Type"] == unit_type) & (one["Delivery_Status"] == "Delivered")]
        assert set(delivered["Project_Completion_Year"]) == set(COMPLETION_YEARS)
        assert set(delivered["Project_Start_Year"]) == set(START_YEARS)
    assert set(one["Borough"]) == {"Brooklyn"}
    assert rows.loc[rows["District_Type"] == "Community_District", "Borough"].notna().all()
    assert rows.loc[rows["District_Type"] == "Council_District", "Borough"].isna().all()
