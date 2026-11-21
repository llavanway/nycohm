"""Combine the district boundary files into the single GeoJSON the dashboard maps.

Each feature keeps two properties, district_type and district_id, which the
dashboard joins to District_Type and District_Value in the main table.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

GEOJSON_DIR = Path(__file__).resolve().parents[3] / "data" / "geojson"

# (file, district_type, property holding the district ID)
DISTRICT_BOUNDARIES = [
    (GEOJSON_DIR / "nycc.geojson", "Council_District", "CounDist"),
    (GEOJSON_DIR / "nycd.geojson", "Community_District", "BoroCD"),
    (GEOJSON_DIR / "nyct2020.geojson", "Census_Tract", "GEOID"),
]

TARGET_CRS = "EPSG:4326"  # WGS84 longitude/latitude, which GeoJSON and Tableau expect
COORDINATE_DECIMALS = 7  # about 1 cm


def combine_geojson(output_path, boundaries=DISTRICT_BOUNDARIES) -> dict:
    """
    Write one WGS84 GeoJSON holding every district boundary.

    Returns the number of features written per district type.
    """
    import shapely
    from pyproj import Transformer
    from shapely.geometry import mapping, shape

    features = []
    counts = {}
    for file_path, district_type, id_property in boundaries:
        with open(file_path) as f:
            collection = json.load(f)

        # GeoJSON is WGS84 unless it says otherwise; the DCP files declare
        # EPSG:2263 (NY State Plane Long Island, US feet)
        source_crs = (
            collection.get("crs", {}).get("properties", {}).get("name") or TARGET_CRS
        )
        transformer = Transformer.from_crs(source_crs, TARGET_CRS, always_xy=True)

        def to_wgs84(coords, transformer=transformer):
            lon, lat = transformer.transform(coords[:, 0], coords[:, 1])
            return np.round(np.column_stack([lon, lat]), COORDINATE_DECIMALS)

        for feature in collection["features"]:
            geometry = shapely.transform(shape(feature["geometry"]), to_wgs84)
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "district_type": district_type,
                        "district_id": str(feature["properties"][id_property]),
                    },
                    "geometry": mapping(geometry),
                }
            )
        counts[district_type] = len(collection["features"])

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f)
    return counts
