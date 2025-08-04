import pandas as pd
import logging
from tabulate import tabulate

'HousingDB keys: CommntyDst,CouncilDst,CenBlock20,CenTract20; CommntyDst: 101.0 ; CouncilDst: 1.0'
'Affordable keys: Community Board, Council District, Census Tract; Community Board: MN-11; Council District: 8.0; Census Tract: 242.0'
'School keys: Geo Dist (school dist.)'
'Transit keys: Census ID'

'''
'school_cap_df['SchoolCapacity'] = school_cap_df['Org Target Cap'] - school_cap_df['Org Enroll']'
'''

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("join.log"),
        logging.StreamHandler()
    ]
)

# Set Pandas to display all columns
pd.set_option('display.max_columns', None)      # Show all columns
pd.set_option('display.width', 25)            # Avoid line breaks
pd.set_option('display.max_colwidth', 25)     # Show full column values (not truncated)

# Load housing metrics
housing_data = pd.read_csv('../../data/HousingDB_post2010.csv')
affordable_housing = pd.read_csv('../../data/Affordable_Housing_Production_by_Building_20250731.csv')
school = pd.read_csv('../../data/Enrollment_Capacity_And_Utilization_Reports_20250731.csv')
transit = pd.read_csv('../../data/New York_36_transit_census_tract_2022.csv')

# Load crosswalk files
community_to_council = pd.read_csv('../../data/community_to_council.csv')
community_to_school = pd.read_csv('../../data/community_to_school.csv')
community_to_tract = pd.read_csv('../../data/community_to_tract.csv')

# log imported data
logging.info("housing_data: %s", tabulate(housing_data.head(5)))
logging.info("affordable_housing: %s", tabulate(affordable_housing.head(5)))

logging.info("housing_data: %s", housing_data.head(1).T.to_string(max_cols=None, line_width=25))
logging.info("affordable_housing: %s", affordable_housing.head(1).T.to_string(max_cols=None, line_width=25))

# join housing to affordable housing
all_housing = housing_data.merge(affordable_housing, left_on='CouncilDst', right_on='Council District', how='left')
logging.info("all_housing: %s", tabulate(all_housing.head(5)))

# join all_housing to school capacity
housing_plus_school = all_housing.merge(community_to_school, left_on='CommntyDst', right_on='BoroCD',how='left')
housing_plus_school = housing_plus_school.merge(school, left_on='SchoolDist', right_on='Geo Dist', how='left')

#
# # Join housing data with crosswalks
# housing_with_community = housing_data.merge(community_to_tract, on='community_id', how='left')
# housing_with_council = housing_with_community.merge(community_to_council, on='community_id', how='left')
# housing_with_school = housing_with_council.merge(community_to_school, on='community_id', how='left')
#
# # Join affordable housing data
# final_dataset = housing_with_school.merge(affordable_housing, on='building_id', how='left')
#
# # Handle missing data
# final_dataset.fillna('Unknown', inplace=True)
#
# final_dataset.head(5)
#
# # Save the unified dataset
# # final_dataset.to_csv('../../data/unified_housing_dataset.csv', index=False)