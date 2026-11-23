"""Names and descriptions for the districts shown on the dashboard.

The pipeline keys every district by an ID (council district "33", community
district "301", census tract "36047055500"). Those IDs are what the map joins
on, but they mean little to a reader, so each district also gets:

    District_Label   a unique readable name
        "Council District 33 (Lincoln Restler)"
        "Brooklyn Community District 1"
        "Brooklyn Census Tract 555"
    Council_Member   the sitting member, for council districts
    Neighborhoods    the neighborhoods the district covers
    Residential      whether people live there (parks, airports, cemeteries and
                     other non-residential areas are False)

Labels are unique across all three district types, so the dashboard can use
the label alone to identify a district.
"""

from __future__ import annotations

import re

import pandas as pd

DISTRICT_COLUMNS = [
    "District_Type",
    "District_Value",
    "District_Label",
    "Council_Member",
    "Neighborhoods",
    "Residential",
]
UNASSIGNED_LABEL = "Unassigned"
NO_MEMBER_LISTED = "no member listed"

# A council district's neighborhoods are the residential neighborhoods (NTAs)
# holding at least this share of its population, largest first.
NEIGHBORHOOD_MIN_SHARE = 0.10
NEIGHBORHOOD_MAX_COUNT = 4

# CDTA code prefix -> (borough code, borough name)
_CDTA_BOROUGH = {
    "MN": (1, "Manhattan"),
    "BX": (2, "Bronx"),
    "BK": (3, "Brooklyn"),
    "QN": (4, "Queens"),
    "SI": (5, "Staten Island"),
}
# CDTAType 'CD' is a community district; 'JIA' is a joint interest area, one of
# the large parks and airports that sit outside the community districts
_CDTA_KIND = {"CD": "Community District", "JIA": "Joint Interest Area"}
_RESIDENTIAL_NTA_TYPE = "0"  # other NTA types are parks, cemeteries, airports and the like

_FALLBACK_PREFIX = {
    "Council_District": "Council District",
    "Community_District": "Community District",
    "Census_Tract": "Census Tract",
}


def _cdta_place_name(cdta_name: str) -> str:
    """'BX01 Melrose-Mott Haven (CD 1 Approximation)' -> 'Melrose-Mott Haven'."""
    name = re.sub(r"^\S+\s+", "", str(cdta_name).strip())
    name = re.sub(r"\s*\((?:CD|JIA) \d+ (?:Approximation|Equivalent)\)\s*$", "", name)
    return name.strip()


def _tract_number(geoid: str) -> str:
    """'36005001901' -> '19.01'; '36005002300' -> '23'."""
    tract6 = str(geoid)[5:]
    base, suffix = int(tract6[:4]), tract6[4:]
    return str(base) if suffix == "00" else f"{base}.{suffix}"


def _council_neighborhoods(tracts: pd.DataFrame) -> pd.Series:
    """
    Neighborhood list per council district, e.g. 'Greenpoint, Williamsburg'.

    tracts: one row per census tract with Council_District, NTAName,
    residential_nta (bool) and population.
    """
    residential = tracts[tracts["residential_nta"] & tracts["Council_District"].notna()]
    by_nta = (
        residential.groupby(["Council_District", "NTAName"], as_index=False)["population"].sum()
    )
    by_nta["share"] = by_nta["population"] / by_nta.groupby("Council_District")[
        "population"
    ].transform("sum")
    by_nta = by_nta.sort_values(
        ["Council_District", "population", "NTAName"], ascending=[True, False, True]
    )
    # always keep a district's largest neighborhood, then any others that are big enough
    rank = by_nta.groupby("Council_District").cumcount()
    keep = (rank == 0) | ((by_nta["share"] >= NEIGHBORHOOD_MIN_SHARE) & (rank < NEIGHBORHOOD_MAX_COUNT))
    return by_nta[keep].groupby("Council_District")["NTAName"].agg(", ".join)


def build_district_labels(
    tract_crosswalk: pd.DataFrame,
    tract_to_council: pd.DataFrame,
    population: pd.DataFrame,
    council_members: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one row per district.

    tract_crosswalk: DCP's 2020 census tract to NTA/CDTA relationship file
        (GEOID, BoroName, NTAName, NTAType, CDTACode, CDTAType, CDTAName).
    tract_to_council: Census_Tract, Council_District.
    population: Census_Tract, P1_001N (2020 census population).
    council_members: Council_District, Council_Member for the sitting members.
    """
    cw = tract_crosswalk.copy()
    cw["GEOID"] = cw["GEOID"].astype("Int64").astype(str)
    cw["residential_nta"] = (
        pd.to_numeric(cw["NTAType"], errors="coerce").astype("Int64").astype(str)
        == _RESIDENTIAL_NTA_TYPE
    )
    pop = population.assign(Census_Tract=population["Census_Tract"].astype(str))
    cw = cw.merge(
        pop[["Census_Tract", "P1_001N"]].rename(columns={"Census_Tract": "GEOID", "P1_001N": "population"}),
        on="GEOID",
        how="left",
    )
    cw["population"] = pd.to_numeric(cw["population"], errors="coerce").fillna(0)

    tracts = pd.DataFrame(
        {
            "District_Type": "Census_Tract",
            "District_Value": cw["GEOID"],
            "District_Label": [
                f"{boro} Census Tract {_tract_number(geoid)}"
                for boro, geoid in zip(cw["BoroName"], cw["GEOID"])
            ],
            "Council_Member": pd.NA,
            "Neighborhoods": cw["NTAName"],
            "Residential": cw["residential_nta"] & (cw["population"] > 0),
        }
    )

    # A CDTA code is the borough abbreviation plus the district number. Take the
    # borough from the code, not from the tract: a few tracts sit in another
    # borough's district (Rikers Island is in the Bronx but in Queens CD 1).
    cd = cw.drop_duplicates(subset="CDTACode")
    cd_code = cd["CDTACode"].astype(str)
    cd_boro = cd_code.str[:2].map(_CDTA_BOROUGH)
    cd_number = cd_code.str[2:].astype(int)
    community = pd.DataFrame(
        {
            "District_Type": "Community_District",
            "District_Value": [
                f"{boro_code}{number:02d}" for (boro_code, _), number in zip(cd_boro, cd_number)
            ],
            "District_Label": [
                f"{boro_name} {_CDTA_KIND.get(kind, kind)} {number}"
                for (_, boro_name), kind, number in zip(cd_boro, cd["CDTAType"], cd_number)
            ],
            "Council_Member": pd.NA,
            "Neighborhoods": [_cdta_place_name(name) for name in cd["CDTAName"]],
            "Residential": (cd["CDTAType"] == "CD").to_numpy(),
        }
    )

    to_council = tract_to_council.assign(
        GEOID=tract_to_council["Census_Tract"].astype(str),
        Council_District=pd.to_numeric(tract_to_council["Council_District"], errors="coerce").astype("Int64"),
    )[["GEOID", "Council_District"]]
    council_tracts = cw.merge(to_council, on="GEOID", how="inner")
    neighborhoods = _council_neighborhoods(council_tracts)
    members = dict(
        zip(
            pd.to_numeric(council_members["Council_District"], errors="coerce").astype("Int64"),
            council_members["Council_Member"],
        )
    )
    council_numbers = sorted(int(n) for n in to_council["Council_District"].dropna().unique())
    council = pd.DataFrame(
        {
            "District_Type": "Council_District",
            "District_Value": [str(n) for n in council_numbers],
            "District_Label": [
                f"Council District {n} ({members.get(n) or NO_MEMBER_LISTED})" for n in council_numbers
            ],
            "Council_Member": [members.get(n, pd.NA) for n in council_numbers],
            "Neighborhoods": [neighborhoods.get(n, pd.NA) for n in council_numbers],
            "Residential": True,
        }
    )

    districts = pd.concat([council, community, tracts], ignore_index=True)[DISTRICT_COLUMNS]
    districts["Residential"] = districts["Residential"].astype(bool)
    for column in ("Council_Member", "Neighborhoods"):
        districts[column] = districts[column].astype("object").astype("string")
    duplicated = districts["District_Label"].duplicated(keep=False)
    if duplicated.any():
        raise ValueError(
            "District labels must be unique: "
            f"{sorted(districts.loc[duplicated, 'District_Label'].unique())[:5]}"
        )
    return districts


def add_district_labels(df: pd.DataFrame, districts: pd.DataFrame) -> pd.DataFrame:
    """
    Add District_Label, Council_Member and Neighborhoods to a frame keyed by
    District_Type and District_Value. Rows with no district get the label
    'Unassigned'; a district missing from the table falls back to its type and
    ID, e.g. 'Census Tract 123'.
    """
    columns = ["District_Type", "District_Value", "District_Label", "Council_Member", "Neighborhoods"]
    out = df.merge(districts[columns], on=["District_Type", "District_Value"], how="left")
    has_value = out["District_Value"].notna() & ~out["District_Value"].astype(str).isin(
        ["", "<NA>", "nan", "None"]
    )
    fallback = out["District_Type"].map(_FALLBACK_PREFIX).fillna("District") + " " + out[
        "District_Value"
    ].astype(str)
    label = out["District_Label"].astype("object")
    label = label.where(label.notna(), fallback)
    out["District_Label"] = label.where(has_value, UNASSIGNED_LABEL).astype("string")
    return out


# --- districts with no housing activity ------------------------------------

_COUNTY_BOROUGH = {
    "005": "Bronx",
    "047": "Brooklyn",
    "061": "Manhattan",
    "081": "Queens",
    "085": "Staten Island",
}
_BOROUGH_BY_CODE = {str(code): name for code, name in _CDTA_BOROUGH.values()}
UNIT_TYPES = ["New Units", "Preserved Units"]


def _district_borough(district_type: str, district_value: str):
    if district_type == "Census_Tract":
        return _COUNTY_BOROUGH.get(str(district_value)[2:5], pd.NA)
    if district_type == "Community_District":
        return _BOROUGH_BY_CODE.get(str(district_value)[:1], pd.NA)
    return pd.NA  # council districts can cross borough lines


def placeholder_years(start_years, completion_years) -> pd.DataFrame:
    """
    The (Delivery_Status, Project_Start_Year, Project_Completion_Year)
    combinations that zero-unit rows are written for.

    The dashboard has a start-year filter and a completion-year filter, and a
    zero row only survives a filter if it carries a selected year. So there is
    a completed row for every start year and for every completion year in the
    data, and an in-progress row (no completion year) for every start year.
    Only years already in the data are used, so the filters gain no new choices.

    This keeps a district on the map whenever one year filter is used. It is
    not every start/completion pair, which would be about seven times as many
    rows: with both filters set to ranges that share no year, a district with
    no activity can still drop out.
    """
    starts = sorted({int(y) for y in start_years if pd.notna(y)})
    completions = sorted({int(y) for y in completion_years if pd.notna(y)})
    if not starts or not completions:
        # nothing to align with: fall back to rows with no year at all
        return pd.DataFrame(
            {
                "Delivery_Status": ["Delivered", "In Progress"],
                "Project_Start_Year": pd.array([pd.NA, pd.NA], dtype="Int64"),
                "Project_Completion_Year": pd.array([pd.NA, pd.NA], dtype="Int64"),
            }
        )

    delivered = set()
    for start in starts:
        # a project cannot complete before it starts, or before the data begins
        later = [c for c in completions if c >= start]
        delivered.add((start, later[0] if later else completions[-1]))
    for completion in completions:
        earlier = [s for s in starts if s <= completion]
        delivered.add((earlier[-1] if earlier else starts[0], completion))

    rows = [("Delivered", start, completion) for start, completion in sorted(delivered)]
    rows += [("In Progress", start, pd.NA) for start in starts]
    years = pd.DataFrame(
        rows, columns=["Delivery_Status", "Project_Start_Year", "Project_Completion_Year"]
    )
    years["Project_Start_Year"] = years["Project_Start_Year"].astype("Int64")
    years["Project_Completion_Year"] = years["Project_Completion_Year"].astype("Int64")
    return years


def placeholder_rows(districts: pd.DataFrame, start_years=(), completion_years=()) -> pd.DataFrame:
    """
    Zero-unit rows for every residential district: one per unit type for each
    combination from placeholder_years().

    The dashboard only draws a district that has rows for the metric and years
    being shown, so a district that produced nothing would otherwise be missing
    from the map instead of showing as zero. Non-residential areas get no rows
    and stay blank. Is_Placeholder marks these rows so that charts where a zero
    row would mislead (a trend line, a borough total) can leave them out.
    """
    residential = districts.loc[districts["Residential"], ["District_Type", "District_Value"]]
    combos = placeholder_years(start_years, completion_years).merge(
        pd.DataFrame({"Unit_Type": UNIT_TYPES}), how="cross"
    )
    rows = residential.merge(combos, how="cross")
    boroughs = {
        key: _district_borough(*key)
        for key in zip(residential["District_Type"], residential["District_Value"])
    }
    rows["Borough"] = [boroughs[key] for key in zip(rows["District_Type"], rows["District_Value"])]
    rows["Housing_Units"] = pd.array([0] * len(rows), dtype="Int64")
    rows["Housing_Units_Affordable"] = pd.array([0] * len(rows), dtype="Int64")
    rows["Is_Placeholder"] = 1
    return rows
