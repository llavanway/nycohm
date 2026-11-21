"""Build 11-digit 2020 census tract GEOIDs for building-level records.

The tract is taken from the building's coordinates (point-in-polygon against
the 2020 tract boundaries) wherever coordinates exist. HPD's own tract column
is only decoded as a fallback, because it is lossy: it drops leading zeros and
drops the two-digit suffix when the suffix is "00", so "1502" can mean tract
15.02 ("001502") or tract 1502 ("150200"). The decoder resolves that by
keeping whichever reading is a real 2020 tract in the borough's county.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

BORO_TO_COUNTY = {
    "Bronx": "005",
    "Manhattan": "061",
    "Brooklyn": "047",
    "Queens": "081",
    "Staten Island": "085",
}

STATE_FIPS = "36"

TRACTS_GEOJSON = (
    Path(__file__).resolve().parents[3] / "data/geojson/nyct2020.geojson"
)

_NULL_STRINGS = {"", "nan", "na", "<na>", "none", "null"}


@dataclass(frozen=True, eq=False)
class TractReference:
    """2020 census tract boundaries plus the set of valid GEOIDs."""

    geoids: np.ndarray  # 11-digit GEOID per polygon, sorted
    geometries: np.ndarray  # shapely geometries, same order as geoids
    crs: str  # CRS of the geometries, e.g. "EPSG:2263"
    valid_geoids: frozenset

    @property
    def tree(self):
        return _tree(self)


@lru_cache(maxsize=None)
def _tree(reference: TractReference):
    import shapely

    return shapely.STRtree(reference.geometries)


@lru_cache(maxsize=None)
def load_tract_reference(path: Path | str = TRACTS_GEOJSON) -> TractReference:
    """Load tract polygons and GEOIDs from the NYC 2020 census tract GeoJSON."""
    from shapely.geometry import shape

    with open(path) as f:
        collection = json.load(f)

    # GeoJSON defaults to WGS84 when no CRS member is present; the DCP file
    # declares EPSG:2263 (NY State Plane Long Island, US feet).
    crs = (
        collection.get("crs", {}).get("properties", {}).get("name") or "EPSG:4326"
    )

    features = sorted(
        collection["features"], key=lambda feat: feat["properties"]["GEOID"]
    )
    geoids = np.array(
        [feat["properties"]["GEOID"] for feat in features], dtype=object
    )
    geometries = np.array(
        [shape(feat["geometry"]) for feat in features], dtype=object
    )
    return TractReference(
        geoids=geoids,
        geometries=geometries,
        crs=crs,
        valid_geoids=frozenset(geoids),
    )


def tracts_from_coordinates(
    latitude, longitude, reference: TractReference | None = None
) -> pd.Series:
    """
    Return the GEOID of the 2020 tract containing each WGS84 point.

    Points with missing or non-numeric coordinates, or that fall outside every
    tract, get <NA>. The result keeps the index of ``latitude``.
    """
    import shapely
    from pyproj import Transformer

    reference = reference or load_tract_reference()

    lat = pd.to_numeric(pd.Series(latitude), errors="coerce")
    lon = pd.to_numeric(pd.Series(longitude), errors="coerce")
    out = pd.Series(pd.NA, index=lat.index, dtype="object")

    lat_values = lat.to_numpy(dtype="float64", na_value=np.nan)
    lon_values = lon.to_numpy(dtype="float64", na_value=np.nan)
    has_point = np.isfinite(lat_values) & np.isfinite(lon_values)
    if not has_point.any():
        return out

    transformer = Transformer.from_crs("EPSG:4326", reference.crs, always_xy=True)
    x, y = transformer.transform(lon_values[has_point], lat_values[has_point])
    located = np.isfinite(x) & np.isfinite(y)
    positions = np.flatnonzero(has_point)[located]
    points = shapely.points(x[located], y[located])

    point_idx, tract_idx = reference.tree.query(points, predicate="intersects")
    # A point exactly on a shared boundary intersects two tracts. Geometries
    # are sorted by GEOID, so keeping the lowest tract index is deterministic.
    order = np.lexsort((tract_idx, point_idx))
    point_idx, tract_idx = point_idx[order], tract_idx[order]
    first = np.ones(len(point_idx), dtype=bool)
    first[1:] = point_idx[1:] != point_idx[:-1]

    out.iloc[positions[point_idx[first]]] = reference.geoids[tract_idx[first]]
    return out


def _tract6_candidates(val) -> list[str]:
    """
    Possible 6-digit tract codes for one raw tract value.

    - An explicit decimal such as 245.01 or '245.1' is unambiguous: 4-digit
      base + 2-digit suffix ('024501', '024510').
    - A whole number (239, 24501, 24501.0, '1502') is a code with leading
      zeros dropped and possibly a dropped '00' suffix, so codes of four
      digits or fewer have two readings: base + '00' and zero-padded to six.
    """
    if pd.isna(val):
        return []

    if isinstance(val, (int, np.integer)) and not isinstance(val, (bool, np.bool_)):
        s = str(int(val))
    elif isinstance(val, (float, np.floating)):
        # pandas reads the HPD column as float64, so 24501 arrives as 24501.0.
        s = str(int(val)) if float(val).is_integer() else repr(float(val))
    else:
        s = str(val).strip()
        if s.lower() in _NULL_STRINGS:
            return []

    match = re.fullmatch(r"(\d+)(?:\.(\d*))?", s)
    if match is None:
        return []
    digits, fraction = match.group(1), match.group(2) or ""

    if fraction.strip("0"):
        if len(digits) > 4 or len(fraction) > 2:
            return []
        return [digits.zfill(4) + fraction.ljust(2, "0")]

    if len(digits) > 6:
        return []
    candidates = [digits.zfill(6)]
    if len(digits) <= 4:
        candidates.insert(0, digits.zfill(4) + "00")
    return list(dict.fromkeys(candidates))


def decode_tract_code(val, county_fips, valid_geoids) -> str | type(pd.NA):
    """
    Decode a raw HPD tract value to an 11-digit GEOID, validated against the
    set of real tracts.

    Returns <NA> when the value is null/unparseable, when no reading is a real
    tract in the county, or when more than one reading is (ambiguous).
    """
    if pd.isna(county_fips):
        return pd.NA
    matches = {
        geoid
        for tract6 in _tract6_candidates(val)
        if (geoid := f"{STATE_FIPS}{county_fips}{tract6}") in valid_geoids
    }
    return matches.pop() if len(matches) == 1 else pd.NA


def make_full_census_tract(
    df,
    tract_col="Census_Tract",
    borough_col="Borough",
    lat_col="Latitude",
    lon_col="Longitude",
    reference: TractReference | None = None,
):
    """
    Adds:
      - Census_Tract_Full: 11-digit 2020 tract GEOID (or <NA> if not derivable)
      - Census_Tract6: 6-digit tract code (string)
      - County_FIPS: 3-digit county code
      - Census_Tract_Source: 'coordinates', 'tract_code', or <NA>

    The GEOID comes from the tract polygon containing (lat_col, lon_col) when
    the row has coordinates, otherwise from decoding tract_col within the
    borough's county. Every non-null GEOID exists in the 2020 tract list; rows
    with no usable tract information are left as <NA> instead of erroring.
    """
    reference = reference or load_tract_reference()
    out = df.copy()

    if lat_col in df.columns and lon_col in df.columns:
        geoid = tracts_from_coordinates(df[lat_col], df[lon_col], reference)
    else:
        geoid = pd.Series(pd.NA, index=df.index, dtype="object")
    source = pd.Series(pd.NA, index=df.index, dtype="object")
    source[geoid.notna()] = "coordinates"

    borough_county = df[borough_col].map(BORO_TO_COUNTY)
    needs_decode = geoid.isna()
    if needs_decode.any():
        decoded = pd.Series(
            [
                decode_tract_code(val, county, reference.valid_geoids)
                for val, county in zip(
                    df.loc[needs_decode, tract_col], borough_county[needs_decode]
                )
            ],
            index=df.index[needs_decode],
            dtype="object",
        )
        geoid[needs_decode] = decoded
        source[needs_decode & geoid.notna()] = "tract_code"

    has_geoid = geoid.notna()
    county = borough_county.astype("object").where(borough_county.notna(), pd.NA)
    county[has_geoid] = geoid[has_geoid].str[2:5]
    tract6 = pd.Series(pd.NA, index=df.index, dtype="object")
    tract6[has_geoid] = geoid[has_geoid].str[5:]

    out["County_FIPS"] = county
    out["Census_Tract6"] = tract6
    out["Census_Tract_Full"] = geoid
    out["Census_Tract_Source"] = source
    return out
