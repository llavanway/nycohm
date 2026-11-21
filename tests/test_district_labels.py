from pathlib import Path

import pandas as pd
import pytest

from src.nycohm.helpers.district_labels import (
    UNASSIGNED_LABEL,
    add_district_labels,
    build_district_labels,
)

CROSSWALK_CSV = (
    Path(__file__).resolve().parents[1]
    / "data/crosswalk/nyc_2020_census_tract_nta_cdta_relationships.csv"
)


@pytest.fixture(scope="module")
def labels():
    # default dtypes, as the ingest asset reads it: GEOID and BoroCode become integers
    return build_district_labels(pd.read_csv(CROSSWALK_CSV), council_districts=range(1, 52))


def _label(labels, district_type, value):
    match = labels[(labels["District_Type"] == district_type) & (labels["District_Value"] == value)]
    assert len(match) == 1
    return match["District_Label"].iloc[0]


def test_one_unique_label_per_district(labels):
    assert labels["District_Type"].value_counts().to_dict() == {
        "Census_Tract": 2327,
        "Community_District": 71,
        "Council_District": 51,
    }
    assert labels["District_Label"].is_unique
    assert not labels.duplicated(subset=["District_Type", "District_Value"]).any()


@pytest.mark.parametrize(
    "district_type, value, expected",
    [
        ("Council_District", "33", "Council District 33"),
        ("Community_District", "301", "Brooklyn CD 1 (Williamsburg-Greenpoint)"),
        ("Community_District", "112", "Manhattan CD 12 (Washington Heights-Inwood)"),
        # joint interest areas (parks, airports) are not community districts
        ("Community_District", "164", "Manhattan JIA 64 (Central Park)"),
        # Rikers Island is a Bronx tract in Queens CD 1; it must not rename Bronx CD 1
        ("Community_District", "201", "Bronx CD 1 (Melrose-Mott Haven-Port Morris)"),
        ("Community_District", "401", "Queens CD 1 (Astoria-Queensbridge)"),
        ("Census_Tract", "36005001901", "Bronx Tract 19.01 (Mott Haven-Port Morris)"),
        ("Census_Tract", "36005002300", "Bronx Tract 23 (Mott Haven-Port Morris)"),
    ],
)
def test_label_formats(labels, district_type, value, expected):
    assert _label(labels, district_type, value) == expected


def test_add_district_labels_fallbacks(labels):
    df = pd.DataFrame(
        {
            "District_Type": ["Council_District", "Census_Tract", "Census_Tract", "Community_District"],
            "District_Value": ["33", "99999999999", None, "<NA>"],
            "Housing_Units": [1, 2, 3, 4],
        }
    )
    out = add_district_labels(df, labels)
    assert out["District_Label"].tolist() == [
        "Council District 33",
        "Census Tract 99999999999",  # not in the crosswalk
        UNASSIGNED_LABEL,
        UNASSIGNED_LABEL,
    ]
    assert out["Housing_Units"].tolist() == [1, 2, 3, 4]
