from pathlib import Path
from src.nycohm.helpers.log_config import configure_logging
import pandas as pd
import geopandas as gpd

configure_logging()

upper_path = Path(__file__).resolve().parents[3] / 'data/geojson/'


def combine_geojson(geojson_specs, output_path):
    """
       Combine multiple district-level GeoJSONs for Tableau.

       Parameters
       ----------
       geojson_specs : list of tuples
           Each tuple = (file_path, district_type_label, id_column_name)
       """
    gdfs = []
    target_crs = "EPSG:4326"

    for file_path, district_type, id_col in geojson_specs:
        gdf = gpd.read_file(file_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(target_crs)
        elif str(gdf.crs).upper() != target_crs:
            gdf = gdf.to_crs(target_crs)
        gdf["district_type"] = district_type
        gdf["district_id"] = gdf[id_col].astype(str)
        gdfs.append(gdf[["district_type", "district_id", "geometry"]])

    combined = pd.concat(gdfs, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=target_crs)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_file(output_path, driver="GeoJSON")
    return output_path

combine_geojson(
    [
        (upper_path / "nycc.geojson", "Council_District", "CounDist"),
        (upper_path / "nycd.geojson", "Community_District", "BoroCD"),
        (upper_path / "nyct2020.geojson", "Census_Tract", "GEOID")
    ],
    upper_path / "nyc_combined.geojson"
)