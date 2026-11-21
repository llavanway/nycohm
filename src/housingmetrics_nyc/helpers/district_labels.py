"""Human-readable names for the districts shown on the dashboard.

The pipeline keys every district by an ID (council district "33", community
district "301", census tract "36047055500"). Those IDs are what the map joins
on, but they mean little to a reader, so each one also gets a label:

    Council_District    "Council District 33"
    Community_District  "Brooklyn CD 1 (Williamsburg-Greenpoint)"
    Census_Tract        "Brooklyn Tract 555 (Williamsburg)"

Labels are unique across all three district types, so the dashboard can use
the label alone to identify a district.
"""

from __future__ import annotations

import re

import pandas as pd

LABEL_COLUMNS = ["District_Type", "District_Value", "District_Label"]
UNASSIGNED_LABEL = "Unassigned"

# CDTA code prefix -> (borough code, borough name)
_CDTA_BOROUGH = {
    "MN": (1, "Manhattan"),
    "BX": (2, "Bronx"),
    "BK": (3, "Brooklyn"),
    "QN": (4, "Queens"),
    "SI": (5, "Staten Island"),
}

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


def build_district_labels(
    tract_crosswalk: pd.DataFrame, council_districts
) -> pd.DataFrame:
    """
    Build one label per district.

    tract_crosswalk: DCP's 2020 census tract to NTA/CDTA relationship file
        (GEOID, BoroCode, BoroName, NTAName, CDTACode, CDTAType, CDTAName).
    council_districts: iterable of council district numbers.
    """
    cw = tract_crosswalk.copy()
    cw["GEOID"] = cw["GEOID"].astype("Int64").astype(str)

    tracts = pd.DataFrame(
        {
            "District_Type": "Census_Tract",
            "District_Value": cw["GEOID"],
            "District_Label": [
                f"{boro} Tract {_tract_number(geoid)} ({nta})"
                for boro, geoid, nta in zip(cw["BoroName"], cw["GEOID"], cw["NTAName"])
            ],
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
                f"{boro_code}{number:02d}"
                for (boro_code, _), number in zip(cd_boro, cd_number)
            ],
            # CDTAType is 'CD' for community districts and 'JIA' for the joint
            # interest areas (large parks and airports) that sit outside them
            "District_Label": [
                f"{boro_name} {kind} {number} ({_cdta_place_name(name)})"
                for (_, boro_name), kind, number, name in zip(
                    cd_boro, cd["CDTAType"], cd_number, cd["CDTAName"]
                )
            ],
        }
    )

    council_numbers = sorted({int(value) for value in council_districts if pd.notna(value)})
    council = pd.DataFrame(
        {
            "District_Type": "Council_District",
            "District_Value": [str(number) for number in council_numbers],
            "District_Label": [f"Council District {number}" for number in council_numbers],
        }
    )

    labels = pd.concat([council, community, tracts], ignore_index=True)[LABEL_COLUMNS]
    duplicated = labels["District_Label"].duplicated(keep=False)
    if duplicated.any():
        raise ValueError(
            "District labels must be unique: "
            f"{sorted(labels.loc[duplicated, 'District_Label'].unique())[:5]}"
        )
    return labels


def add_district_labels(df: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """
    Add a District_Label column to a frame keyed by District_Type and
    District_Value. Rows with no district get 'Unassigned'; a district missing
    from the label table falls back to its type and ID, e.g. 'Census Tract 123'.
    """
    out = df.merge(labels[LABEL_COLUMNS], on=["District_Type", "District_Value"], how="left")
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
