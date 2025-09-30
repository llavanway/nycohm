
from __future__ import annotations
from connect_bq import connect_bq

import json
import os
from pathlib import Path
import pyproj

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# set GeoJSON file paths
geojson_paths = {
    "nycd": {"table_name": "nycd_manual_test","path": Path(__file__).resolve().parents[3] / "data" / "geojson" / "nycd.geojson"},
    "nycc": {"table_name": "nycc_manual_test","path": Path(__file__).resolve().parents[3] / "data" / "geojson" / "nycc.geojson"},
    "nyct": {"table_name": "nyct_manual_test","path": Path(__file__).resolve().parents[3] / "data" / "geojson" / "nyct2020.geojson"}
}

# geojson_paths = [
# Path(__file__).resolve().parents[3] / "data" / "geojson" / "nycd.geojson",
# Path(__file__).resolve().parents[3] / "data" / "geojson" / "nycc.geojson",
# Path(__file__).resolve().parents[3] / "data" / "geojson" / "nyct2020.geojson"
# ]

def convert_ny_state_plane_to_latlon(file_path):
    """
    Convert GeoJSON from EPSG:2263 (NY Long Island State Plane) to WGS84 lat/lon
    """
    # EPSG:2263 is NAD83 / New York Long Island (ftUS) - uses US Survey Feet
    transformer = pyproj.Transformer.from_crs(
        'EPSG:2263',  # NY Long Island State Plane (US Survey Feet)
        'EPSG:4326',  # WGS84 lat/lon
        always_xy=True
    )

    # Load the GeoJSON
    with file_path.open() as f:
        geojson = json.load(f)

    def transform_coordinates(coords):
        if isinstance(coords[0], (int, float)):
            # Single coordinate pair [x, y] in US Survey Feet
            lon, lat = transformer.transform(coords[0], coords[1])
            return [lon, lat]
        else:
            # Nested coordinate array
            return [transform_coordinates(coord) for coord in coords]

    # Transform all geometries
    if geojson['type'] == 'FeatureCollection':
        for feature in geojson['features']:
            if feature.get('geometry') and feature['geometry'].get('coordinates'):
                feature['geometry']['coordinates'] = transform_coordinates(
                    feature['geometry']['coordinates']
                )
    elif geojson['type'] == 'Feature':
        if geojson.get('geometry') and geojson['geometry'].get('coordinates'):
            geojson['geometry']['coordinates'] = transform_coordinates(
                geojson['geometry']['coordinates']
            )
    else:
        # Direct geometry
        if geojson.get('coordinates'):
            geojson['coordinates'] = transform_coordinates(geojson['coordinates'])

    # Save the converted GeoJSON
    # with open(output_file, 'w') as f:
    #     json.dump(geojson, f, indent=2)
    #
    # print(f"Successfully converted from EPSG:2263 to WGS84 lat/lon")
    # print(f"Output saved to: {output_file}")

    return geojson


def upload_nycd_geojson(
    project: str | None = None,
    dataset: str | None = None,
    table: str | None = None,
    file_path: Path = None,
) -> None:
    """Upload the NYCD GeoJSON file to BigQuery.

    Args:
        project: Target BigQuery project. Defaults to ``BQ_PROJECT`` env var.
        dataset: Target BigQuery dataset. Defaults to ``BQ_DATASET`` env var.
        table: Destination table name. Defaults to ``BQ_TABLE`` env var or
            ``"nycd_geojson_test"``.
        file_path: Path to the GeoJSON file.
    """

    project = project or os.getenv("nycohm", "my-project")
    dataset = dataset or os.getenv("manual_tests", "analytics")
    table = table or os.getenv("nycd_manual_test", "nycd_geojson_test")

    # client = bigquery.Client(project=project)
    client = connect_bq()
    dataset_ref = bigquery.DatasetReference(project, dataset)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        client.create_dataset(bigquery.Dataset(dataset_ref))

    features = convert_ny_state_plane_to_latlon(file_path)["features"]

    rows = [
        {
            "boro_cd": feature["properties"].get("BoroCD"),
            "shape_leng": feature["properties"].get("Shape_Leng"),
            "shape_area": feature["properties"].get("Shape_Area"),
            "geometry": json.dumps(feature["geometry"]),
        }
        for feature in features
    ]

    table_id = f"{project}.{dataset}.{table}"
    job = client.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("boro_cd", "INT64"),
                bigquery.SchemaField("shape_leng", "FLOAT64"),
                bigquery.SchemaField("shape_area", "FLOAT64"),
                bigquery.SchemaField("geometry", "GEOGRAPHY"),
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}.")


if __name__ == "__main__":
    for i in geojson_paths:
        upload_nycd_geojson(project='nycohm', dataset='manual_tests', table=i["table_name"], file_path=i["path"])
