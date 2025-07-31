import os
import geopandas as gpd
import logging
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crosswalk.log"),
        logging.StreamHandler()
    ]
)

# Helper function to construct absolute paths
def get_absolute_path(relative_path):
    path = os.path.join(os.path.dirname(__file__), relative_path)
    return os.path.abspath(path)

# Define shapefile paths
shapefiles = {
    "council": '../../data/shapefiles/nycc/nycc.shp',
    "community": '../../data/shapefiles/nycd/nycd.shp',
    "tract": '../../data/shapefiles/nyct/nyct2020.shp',
    "school": '../../data/shapefiles/nysd/NYC_School_Districts.shp'
}

# Convert to absolute paths and check existence
for key, relative_path in shapefiles.items():
    shapefiles[key] = get_absolute_path(relative_path)
    if not os.path.exists(shapefiles[key]):
        logging.error(f"Shapefile not found: {shapefiles[key]}")
        raise FileNotFoundError(f"Shapefile not found: {shapefiles[key]}")
    logging.info(f"Shapefile found: {shapefiles[key]}")

# Load shapefiles
try:
    logging.info("Loading shapefiles...")
    council_gdf = gpd.read_file(shapefiles["council"])
    community_gdf = gpd.read_file(shapefiles["community"])
    tract_gdf = gpd.read_file(shapefiles["tract"])
    school_gdf = gpd.read_file(shapefiles["school"])
    logging.info("Shapefiles loaded successfully.")
except Exception as e:
    logging.error(f"Error loading shapefiles: {e}")
    raise

# Standardize CRS
try:
    logging.info("Standardizing CRS...")
    target_crs = 'EPSG:2263'
    council_gdf = council_gdf.to_crs(target_crs)
    community_gdf = community_gdf.to_crs(target_crs)
    tract_gdf = tract_gdf.to_crs(target_crs)
    school_gdf = school_gdf.to_crs(target_crs)
    logging.info("CRS standardized successfully.")
except Exception as e:
    logging.error(f"Error standardizing CRS: {e}")
    raise

# Update the output file paths to use absolute paths
output_dir = get_absolute_path('../../data')
logging.info('Output directory: %s', output_dir)

# Perform spatial joins and save outputs
try:
    # logging.info("Performing spatial joins and saving outputs...")
    # tract_to_community = gpd.sjoin(tract_gdf, community_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: tract_to_community completed.")
    # logging.info('tract_to_community columns: %s', tract_to_community.columns)
    # logging.info('tract_to_community head: %s', tabulate(tract_to_community.head(5)))
    # tract_to_community[['CT2020', 'BoroCD']].drop_duplicates().to_csv(os.path.join(output_dir, 'tract_to_community.csv'), index=False)
    #
    # community_to_council = gpd.sjoin(community_gdf, council_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: community_to_council completed.")
    # community_to_council[['BoroCD', 'CounDist']].drop_duplicates().to_csv(os.path.join(output_dir, 'community_to_council.csv'), index=False)
    #
    # tract_to_council = gpd.sjoin(tract_gdf, council_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: tract_to_council completed.")
    # tract_to_council[['CT2020', 'CounDist']].drop_duplicates().to_csv(os.path.join(output_dir, 'tract_to_council.csv'), index=False)
    #
    # tract_to_school = gpd.sjoin(tract_gdf, school_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: tract_to_school completed.")
    # tract_to_school[['CT2020', 'SchoolDist']].drop_duplicates().to_csv(os.path.join(output_dir, 'tract_to_school.csv'), index=False)
    #
    # community_to_school = gpd.sjoin(community_gdf, school_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: community_to_school completed.")
    # community_to_school[['BoroCD', 'SchoolDist']].drop_duplicates().to_csv(os.path.join(output_dir, 'community_to_school.csv'), index=False)
    #
    # council_to_school = gpd.sjoin(council_gdf, school_gdf, how='left', predicate='intersects')
    # logging.info("Spatial join: council_to_school completed.")
    # council_to_school[['CounDist', 'SchoolDist']].drop_duplicates().to_csv(os.path.join(output_dir, 'council_to_school.csv'), index=False)

    # Create community_to_tract mapping based on centroid containment
    logging.info("Creating community_to_tract mapping based on centroid containment...")
    community_gdf['centroid'] = community_gdf.geometry.centroid
    community_centroids = gpd.GeoDataFrame(community_gdf[['BoroCD']], geometry=community_gdf['centroid'],
                                           crs=community_gdf.crs)
    community_to_tract = gpd.sjoin(community_centroids, tract_gdf, how='left', predicate='within')
    logging.info("Spatial join: community_to_tract completed.")
    community_to_tract[['BoroCD', 'CT2020']].drop_duplicates().to_csv(
        os.path.join(output_dir, 'community_to_tract.csv'), index=False)

    # Create council_to_tract mapping based on centroid containment
    logging.info("Creating council_to_tract mapping based on centroid containment...")
    council_gdf['centroid'] = council_gdf.geometry.centroid
    council_centroids = gpd.GeoDataFrame(council_gdf[['CounDist']], geometry=council_gdf['centroid'],
                                         crs=council_gdf.crs)
    council_to_tract = gpd.sjoin(council_centroids, tract_gdf, how='left', predicate='within')
    logging.info("Spatial join: council_to_tract completed.")
    council_to_tract[['CounDist', 'CT2020']].drop_duplicates().to_csv(
        os.path.join(output_dir, 'council_to_tract.csv'), index=False)
    logging.info("Council to tract mapping saved successfully.")

    # Create community_to_school mapping based on centroid containment
    logging.info("Creating community_to_school mapping based on centroid containment...")
    community_gdf['centroid'] = community_gdf.geometry.centroid
    community_centroids = gpd.GeoDataFrame(community_gdf[['BoroCD']], geometry=community_gdf['centroid'],
                                           crs=community_gdf.crs)
    community_to_school = gpd.sjoin(community_centroids, school_gdf, how='left', predicate='within')
    logging.info("Spatial join: community_to_school completed.")
    community_to_school[['BoroCD', 'SchoolDist']].drop_duplicates().to_csv(
        os.path.join(output_dir, 'community_to_school.csv'), index=False)
    logging.info("Community to school mapping saved successfully.")

    # Create council_to_school mapping based on centroid containment
    logging.info("Creating council_to_school mapping based on centroid containment...")
    council_gdf['centroid'] = council_gdf.geometry.centroid
    council_centroids = gpd.GeoDataFrame(council_gdf[['CounDist']], geometry=council_gdf['centroid'],
                                         crs=council_gdf.crs)
    council_to_school = gpd.sjoin(council_centroids, school_gdf, how='left', predicate='within')
    logging.info("Spatial join: council_to_school completed.")
    council_to_school[['CounDist', 'SchoolDist']].drop_duplicates().to_csv(
        os.path.join(output_dir, 'council_to_school.csv'), index=False)
    logging.info("Council to school mapping saved successfully.")

    logging.info("All outputs saved successfully.")
except Exception as e:
    logging.error(f"Error during spatial joins or saving outputs: {e}")
    raise