import pandas as pd
import os
import logging
import geopandas as gpd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def join_housing_datasets(housing_file_path: str, affordable_file_path: str) -> pd.DataFrame:
    """
    Join HousingDB and Affordable Housing datasets based on the BBL key and year of completion.

    Parameters:
    housing_file_path (str): Path to the HousingDB CSV file.
    affordable_file_path (str): Path to the Affordable Housing CSV file.

    Returns:
    pd.DataFrame: Merged DataFrame containing data from both datasets.
    """
    # Load the datasets
    housing_df = pd.read_csv(housing_file_path, low_memory=False)
    affordable_df = pd.read_csv(affordable_file_path, low_memory=False)

    logging.info(f"Shape of housing_df: {housing_df.shape}")
    logging.info(f"Shape of affordable_df: {affordable_df.shape}")

    # Drop affordable housing preservation projects
    logging.info("Dropping affordable housing preservation projects...")
    affordable_df = affordable_df[affordable_df['Reporting Construction Type'] != 'Preservation']

    # Drop rows with missing Year of Completion
    logging.info("Dropping rows with no completion date...")
    housing_df = housing_df.dropna(subset=['CompltYear'])
    affordable_df = affordable_df.dropna(subset=['Project Completion Date'])

    logging.info(f"Shape of housing_df after dropping rows: {housing_df.shape}")
    logging.info(f"Shape of affordable_df after dropping rows: {affordable_df.shape}")

    # Extract year of completion from HousingDB
    housing_df['Year of Completion'] = pd.to_datetime(housing_df['CompltYear'], errors='coerce').dt.year

    # Extract year of completion from Affordable Housing dataset
    affordable_df['Year of Completion'] = pd.to_datetime(affordable_df['Project Completion Date'], errors='coerce').dt.year

    # Aggregate rows missing BBL into rows not missing BBL by matching Project ID
    missing_bbl_rows = affordable_df[affordable_df['BBL'].isna()]
    non_missing_bbl_rows = affordable_df[~affordable_df['BBL'].isna()]
    aggregated_units = missing_bbl_rows.groupby('Project ID')['All Counted Units'].sum()

    non_missing_bbl_rows = non_missing_bbl_rows.merge(
        aggregated_units, on='Project ID', how='left', suffixes=('', '_missing_bbl')
    )
    non_missing_bbl_rows['All Counted Units'] += non_missing_bbl_rows['All Counted Units_missing_bbl'].fillna(0)
    non_missing_bbl_rows.drop(columns=['All Counted Units_missing_bbl'], inplace=True)

    affordable_df = non_missing_bbl_rows

    # Log results of handling rows with missing BBL
    logging.info(f"Rows in affordable_df missing BBL before aggregation: {missing_bbl_rows.shape[0]}")
    logging.info(f"Rows in affordable_df after aggregation: {affordable_df.shape[0]}")

    # Count rows with missing BBL or Year of Completion in housing_df
    missing_housing_bbl = housing_df['BBL'].isna().sum()
    missing_housing_year = housing_df['Year of Completion'].isna().sum()

    logging.info(f"Rows in housing_df missing BBL: {missing_housing_bbl}")
    logging.info(f"Rows in housing_df missing Year of Completion: {missing_housing_year}")

    # Count rows with missing BBL or Year of Completion in affordable_df
    missing_affordable_bbl = affordable_df['BBL'].isna().sum()
    missing_affordable_year = affordable_df['Year of Completion'].isna().sum()

    logging.info(f"Rows in affordable_df missing BBL: {missing_affordable_bbl}")
    logging.info(f"Rows in affordable_df missing Year of Completion: {missing_affordable_year}")

    logging.info(f"housing_df sample: \n {housing_df.sample(10).to_string()}")
    logging.info(f"affordable_df sample: \n {affordable_df.sample(10).to_string()}")

    # Convert BBL columns to string type in both datasets
    housing_df['BBL'] = housing_df['BBL'].astype(str)
    affordable_df['BBL'] = affordable_df['BBL'].astype(str)

    # Log the data types after conversion
    logging.info(f"Data type of housing_df['BBL']: {housing_df['BBL'].dtype}")
    logging.info(f"Data type of affordable_df['BBL']: {affordable_df['BBL'].dtype}")

    # Check data type consistency
    assert housing_df['BBL'].dtype == affordable_df['BBL'].dtype, "BBL columns have inconsistent data types."

    # Perform the join on BBL and Year of Completion
    merged_df = pd.merge(housing_df, affordable_df, on=['BBL', 'Year of Completion'], how='left')

    # Rename key columns for consistency
    merged_df.rename(columns={'community_district_x': 'community_district'}, inplace=True)
    merged_df.rename(columns={'council_district_x': 'council_district'}, inplace=True)

    unjoined_rows = merged_df[merged_df[affordable_df.columns.difference(housing_df.columns)].isna().all(axis=1)]

    logging.info(f"Unjoined rows: {unjoined_rows.shape}")

    # Calculate total housing units and affordable units
    total_housing_units = housing_df['ClassANet'].sum()
    total_affordable_units = affordable_df['All Counted Units'].sum()

    logging.info(f"Total housing units: {total_housing_units}")
    logging.info(f"Total affordable units: {total_affordable_units}")

    return merged_df

# Example usage:
# merged_data = join_housing_datasets(
#     '../../data/standardized/HousingDB_post2010_standardized.csv',
#     '../../data/standardized/Affordable_Housing_Production_by_Building_standardized.csv'
# )
#
# print(merged_data.sample(20).to_string())

# Save the merged data to the standardized directory
# merged_data.to_csv('../../data/standardized/merged_housing_data.csv', index=False)


def aggregate_housing_data(file_path: str) -> pd.DataFrame:
    """
    Aggregates the housing dataset by community_district and council_district,
    breaking out sums of ClassANet and All Counted Units by year, and including
    ZoningDst1 and FloorsProp dimensions.

    Parameters:
    file_path (str): Path to the merged housing dataset CSV file.

    Returns:
    pd.DataFrame: Aggregated DataFrame.
    """
    # Load the dataset
    housing_data = pd.read_csv(file_path)

    # Perform aggregation
    aggregated_data = housing_data.groupby(
        ['community_district', 'council_district', 'Year of Completion', 'ZoningDst1', 'FloorsProp']
    ).agg(
        ClassANet_sum=('ClassANet', 'sum'),
        AllCountedUnits_sum=('All Counted Units', 'sum')
    ).reset_index()

    # Log total sums
    total_classanet_sum = aggregated_data['ClassANet_sum'].sum()
    total_allcountedunits_sum = aggregated_data['AllCountedUnits_sum'].sum()
    logging.info(f"Total ClassANet_sum: {total_classanet_sum}")
    logging.info(f"Total AllCountedUnits_sum: {total_allcountedunits_sum}")

    return aggregated_data

# Example usage:
# aggregated_data = aggregate_housing_data('../../data/standardized/merged_housing_data.csv')
# print(aggregated_data.head(50).to_string())
#
# # Save the aggregated data to the standardized directory
# aggregated_data.to_csv('../../data/standardized/aggregated_housing_data.csv', index=False)

def shp_to_geo(shapefiles_dir: str, geojson_dir: str) -> None:
    """
    Converts all shapefiles in the specified directory to GeoJSON files.

    Parameters:
    shapefiles_dir (str): Path to the directory containing shapefiles.
    geojson_dir (str): Path to the directory where GeoJSON files will be saved.
    """
    # Ensure the output directory exists
    if not os.path.exists(shapefiles_dir):
        logging.error(f"Shapefiles directory does not exist: {shapefiles_dir}")
        return

    os.makedirs(geojson_dir, exist_ok=True)

    # Iterate through all files in the shapefiles directory
    for file_name in os.listdir(shapefiles_dir):
        logging.info(f"Filename: {file_name}")
        if file_name.endswith('.shp'):
            try:
                # Construct full file paths
                shp_path = os.path.join(shapefiles_dir, file_name)
                geojson_path = os.path.join(geojson_dir, file_name.replace('.shp', '.geojson'))

                # Read the shapefile and convert to GeoJSON
                gdf = gpd.read_file(shp_path)
                gdf.to_file(geojson_path, driver='GeoJSON')

                logging.info(f"Converted {file_name} to {geojson_path}")
            except Exception as e:
                logging.error(f"Failed to convert {file_name}: {e}")

# shp_to_geo('../../data/shapefiles/nycc','../../data/geojson')
# shp_to_geo('../../data/shapefiles/nycd','../../data/geojson')
# shp_to_geo('../../data/shapefiles/nyct','../../data/geojson')
# shp_to_geo('../../data/shapefiles/nysd','../../data/geojson')