import numpy as np
import pandas as pd
import pytest

from src.nycohm.fetch_data import dataset_path
from src.nycohm.helpers.make_full_census_tract import (
    decode_tract_code,
    load_tract_reference,
    make_full_census_tract,
    tracts_from_coordinates,
)

# not stored in the repo; download with `python -m src.nycohm.fetch_data`
AFFORDABLE_CSV = dataset_path("affordable_housing_production_by_building")

BRONX, MANHATTAN, BROOKLYN, QUEENS = "005", "061", "047", "081"


@pytest.fixture(scope="module")
def reference():
    return load_tract_reference()


@pytest.fixture(scope="module")
def valid(reference):
    return reference.valid_geoids


def test_reference_is_the_2020_tract_list(reference):
    assert len(reference.geoids) == 2325
    assert len(reference.valid_geoids) == 2325
    assert all(len(g) == 11 and g.startswith("36") for g in reference.geoids)
    assert "2263" in reference.crs


# --- decoding HPD's tract column (fallback path) ---------------------------


@pytest.mark.parametrize(
    "value, county, expected",
    [
        # pd.read_csv yields float64, so codes arrive with a trailing ".0"
        (24501.0, BRONX, "36005024501"),  # tract 245.01, was 360052450100
        (2201.0, MANHATTAN, "36061002201"),  # tract 22.01, was 36061220100
        (239.0, BRONX, "36005023900"),  # tract 239
        (np.float64(24501.0), BRONX, "36005024501"),
        (24501, BRONX, "36005024501"),
        (np.int64(239), BRONX, "36005023900"),
    ],
)
def test_decode_numeric_inputs(valid, value, county, expected):
    assert decode_tract_code(value, county, valid) == expected


@pytest.mark.parametrize(
    "value, county, expected",
    [
        ("24501", BRONX, "36005024501"),
        ("24501.0", BRONX, "36005024501"),
        ("024501", BRONX, "36005024501"),
        ("245.01", BRONX, "36005024501"),
        (" 239 ", BRONX, "36005023900"),
        ("239.0", BRONX, "36005023900"),
        ("239.00", BRONX, "36005023900"),
        ("2201", MANHATTAN, "36061002201"),
        ("22.01", MANHATTAN, "36061002201"),
    ],
)
def test_decode_string_inputs(valid, value, county, expected):
    assert decode_tract_code(value, county, valid) == expected


@pytest.mark.parametrize(
    "value", [None, np.nan, pd.NA, "", "  ", "nan", "<NA>", "None", "n/a", "12-34"]
)
def test_decode_null_and_unparseable_inputs(valid, value):
    assert decode_tract_code(value, BRONX, valid) is pd.NA


def test_decode_null_county(valid):
    assert decode_tract_code(24501.0, pd.NA, valid) is pd.NA
    assert decode_tract_code(24501.0, None, valid) is pd.NA


def test_decode_short_code_picks_the_reading_that_is_a_real_tract(valid):
    # "1502" in Manhattan: tract 15.02 exists, tract 1502 does not.
    assert "36061001502" in valid and "36061150200" not in valid
    assert decode_tract_code("1502", MANHATTAN, valid) == "36061001502"
    assert decode_tract_code(1502.0, MANHATTAN, valid) == "36061001502"

    # "1223" in Queens: tract 1223 exists, tract 12.23 does not.
    assert "36081122300" in valid and "36081001223" not in valid
    assert decode_tract_code("1223", QUEENS, valid) == "36081122300"


def test_decode_ambiguous_short_code_is_null(valid):
    # "1502" in Brooklyn: both tract 15.02 and tract 1502 are real tracts.
    assert {"36047001502", "36047150200"} <= valid
    assert decode_tract_code("1502", BROOKLYN, valid) is pd.NA
    assert decode_tract_code(1502.0, BROOKLYN, valid) is pd.NA


def test_decode_code_matching_no_tract_is_null(valid):
    assert decode_tract_code("1227", BROOKLYN, valid) is pd.NA
    assert decode_tract_code(9999999.0, BRONX, valid) is pd.NA
    assert decode_tract_code("245.011", BRONX, valid) is pd.NA


# --- point-in-polygon on coordinates ---------------------------------------


def test_tracts_from_coordinates(reference):
    lat = pd.Series([40.850665, 40.719598, 40.692038, np.nan, 40.0, None], index=list("abcdef"))
    lon = pd.Series([-73.915259, -73.982363, -73.981948, -73.98, -73.0, None], index=list("abcdef"))
    result = tracts_from_coordinates(lat, lon, reference)

    assert list(result.index) == list("abcdef")
    assert result["a"] == "36005024501"  # 1805 University Avenue, Bronx
    assert result["b"] == "36061002201"  # 210 Stanton Street, Manhattan
    assert result["c"] == "36047001502"  # 165 Willoughby Street, Brooklyn
    assert pd.isna(result["d"])  # missing latitude
    assert pd.isna(result["e"])  # in the Atlantic, outside every tract
    assert pd.isna(result["f"])


def test_tracts_from_coordinates_all_missing(reference):
    result = tracts_from_coordinates(
        pd.Series([pd.NA, None], dtype="object"), pd.Series([pd.NA, None], dtype="object"), reference
    )
    assert result.isna().all()


# --- make_full_census_tract --------------------------------------------------


def test_make_full_census_tract_prefers_coordinates_then_decodes(reference):
    df = pd.DataFrame(
        {
            "Borough": ["Bronx", "Brooklyn", "Bronx", "Manhattan", "Brooklyn", "Bronx", None],
            "Census_Tract": [24501.0, 1502.0, 24501.0, "2201", 1502.0, np.nan, np.nan],
            "Latitude": [40.850665, 40.692038, np.nan, np.nan, np.nan, np.nan, np.nan],
            "Longitude": [-73.915259, -73.981948, np.nan, np.nan, np.nan, np.nan, np.nan],
        },
        index=[10, 11, 12, 13, 14, 15, 16],
    )
    out = make_full_census_tract(df, reference=reference)

    assert list(out.index) == list(df.index)
    assert out["Census_Tract_Full"].tolist()[:4] == [
        "36005024501",  # coordinates
        "36047001502",  # coordinates resolve a code that is ambiguous on its own
        "36005024501",  # no coordinates: float code decoded
        "36061002201",  # no coordinates: string short code decoded
    ]
    assert out["Census_Tract_Full"].iloc[4:].isna().all()  # ambiguous / no info
    assert out["Census_Tract_Source"].tolist()[:4] == [
        "coordinates",
        "coordinates",
        "tract_code",
        "tract_code",
    ]
    assert out["Census_Tract_Source"].iloc[4:].isna().all()
    assert out["Census_Tract6"].tolist()[:4] == ["024501", "001502", "024501", "002201"]
    assert out["County_FIPS"].tolist()[:6] == ["005", "047", "005", "061", "047", "005"]
    assert pd.isna(out["County_FIPS"].iloc[6])
    # input frame is not mutated
    assert "Census_Tract_Full" not in df.columns


def test_make_full_census_tract_without_coordinate_columns(reference):
    df = pd.DataFrame({"Borough": ["Bronx", "Manhattan"], "Census_Tract": [239.0, 2201.0]})
    out = make_full_census_tract(df, reference=reference)
    assert out["Census_Tract_Full"].tolist() == ["36005023900", "36061002201"]


def test_make_full_census_tract_rows_without_any_tract_info_stay_null(reference):
    # CONFIDENTIAL projects have blank tract, BBL and coordinates.
    df = pd.DataFrame(
        {
            "Borough": ["Brooklyn", "Queens"],
            "Census_Tract": [pd.NA, pd.NA],
            "Latitude": [pd.NA, pd.NA],
            "Longitude": [pd.NA, pd.NA],
        },
        dtype="object",
    )
    out = make_full_census_tract(df, reference=reference)
    assert out["Census_Tract_Full"].isna().all()
    assert out["Census_Tract6"].isna().all()
    assert out["County_FIPS"].tolist() == ["047", "081"]


# --- the real HPD file ---------------------------------------------------------


@pytest.fixture(scope="module")
def affordable_raw():
    if not AFFORDABLE_CSV.exists():
        pytest.skip(f"{AFFORDABLE_CSV.name} not downloaded; run python -m src.nycohm.fetch_data")
    return pd.read_csv(AFFORDABLE_CSV)


def test_affordable_dataset_tracts_are_all_valid_2020_geoids(affordable_raw, reference):
    assert affordable_raw["Census Tract"].dtype == "float64"  # the cause of the original bug

    out = make_full_census_tract(affordable_raw, tract_col="Census Tract", reference=reference)
    geoid = out["Census_Tract_Full"]
    assigned = geoid.dropna()

    assert assigned.str.fullmatch(r"\d{11}").all()
    assert assigned.isin(reference.valid_geoids).all()

    # every row with coordinates or an HPD tract code gets a tract ...
    has_info = affordable_raw["Latitude"].notna() | affordable_raw["Census Tract"].notna()
    assert geoid[has_info].notna().all()
    # ... in the county of its borough ...
    county = affordable_raw["Borough"].map(
        {"Bronx": "005", "Manhattan": "061", "Brooklyn": "047", "Queens": "081", "Staten Island": "085"}
    )
    assert (assigned.str[2:5] == county[assigned.index]).all()
    # ... and rows with no tract information stay null.
    confidential = affordable_raw["Project Name"] == "CONFIDENTIAL"
    assert confidential.sum() > 0
    assert geoid[confidential].isna().all()
    assert geoid[~has_info].isna().all()


def test_affordable_clean_asset_census_tract(affordable_raw, reference):
    pytest.importorskip("dagster")
    from src.nycohm.assets.affordable_housing_production_by_building import (
        affordable_housing_production_by_building_clean,
    )
    from src.nycohm.helpers.prep_for_bq import sanitize_bq_columns

    ingested, _ = sanitize_bq_columns(affordable_raw.copy(), lowercase=False)
    df = affordable_housing_production_by_building_clean(ingested).value

    tract = df["Census_Tract"]
    assert len(df) == len(affordable_raw)
    assert tract.notna().sum() == affordable_raw["Latitude"].notna().sum()
    assert tract.dropna().isin(reference.valid_geoids).all()
    # nulls are real nulls, not the strings "nan" / "<NA>"
    assert tract[df["Project_Name"] == "CONFIDENTIAL"].isna().all()
    assert not tract.dropna().isin(["nan", "<NA>", "None"]).any()
    assert "Census_Tract_Full" not in df.columns
    assert "Census_Tract_Source" not in df.columns
